package edu.uvm.banner.general.batch
import java.sql.Connection
import java.util.List;
import java.util.Map;

import groovy.sql.Sql

class LetterService {
	Connection db //Database Connection
	Map jobparms // Job Parameter Map:
	String jobname
	Map jobparm_names 
	/*
 Parameters are as follows:
 01	Email Letter Code	-S-	Letter Code defined for E-Mail letter
 02	Student ID			-S-	ID of Student when running this process for a single ID
 03	Application ID		-S-	General area for which the selection was defined
 04	Selection ID		-S-	Code that identifies the sub-population to work with
 05	Creator ID			-S-	The ID of the person creating the sub-population rules
 06	User ID				-S-	The ID of the person using the sub-population rules
 07	Verbose Output		-S- provide detailed output of the processing.
 08	Email Address Types	-M-	E-Mail Address Types to email to (CSO, AUTH, PY)
 09	Proxy Roles			-M-	Proxy Roles to send the email to.
 10	Proxy Access Level	-S-	View Name Proxy has access to:Any form (%),
								 Faid info (bwrkrhst.P_DispAwdHst),
								 Billing Info (bwsksphs.P_ViewStatement)
 11	Term Code			-S-	Term code for selecting students record for the letter processing.
 12	Audit/Update/Debug	-S-	Enter A to print to log and not update; U to send email and update gurmail;D=debug
 13	Email Subject Text	-S-	Subject Text for Emails.
 14	FROM Email Address	-S-	FROM Email Address. Required for Oracle Mail API.
 15	FROM Name			-S-	Used with From Email Address; include name of sender with email addr.
 16	Mail Host			-S-	This is the mail host to send the email from. ex: mailhost.xxx.edu
 17	Mail Host Port Number-S-Mail Host Port Number from which the email will be sent. Default = 25
 */
 
	// Properties holding letter definition
	// used to generate the emails.
	def letterText
	def letterDynoCols
	Boolean letterUsesBillData
	
	String letter_code
	String letter_module
	String letter_view
	
	String initLetter(){
		// populate static letter information to be reused for each student
		Sql sql = new Sql(db)
		letter_code = jobparms['01']
		Map parms = [letter_code : letter_code]
		letterText = sql.rows(qryLetterText, parms)
		letterUsesBillData = letterText.any { Letter.hasTag( it.SORELTR_TEXT_VAR)}
		letterDynoCols = sql.rows(qryLetterDynoCols, parms)
		def r =  sql.firstRow(qryLetterDynoColsView, parms)
		letter_module = r.stvelmt_code
		letter_view = r.stvelmt_view
		jobparm_names = fetchJobParmNames()
		return (letterText) ? '' : "!!! No letter text found for ${letter_code}"
	}
	
	def countStudents(){
		//count the number of students identified by the selection 
		String sqlexprn = ""
		sqlexprn = [qryStudentById,qryStudentByPop,qryStudentByBill,qryStudentByUnknown][getStudentSource()]
		sqlexprn = cvtSelectToCount(sqlexprn)
		
		Sql sql = new Sql(db)
		//return sql.firstRow(sqlexprn).count
		return sql.firstRow(sqlexprn,studentSelectParms()).count
	}

	def getStudents(){
		//count the number of students identified by the selection
		String sqlexprn = ""
		sqlexprn = [qryStudentById,qryStudentByPop,qryStudentByBill,qryStudentByUnknown][getStudentSource()]
		Sql sql = new Sql(db)
		return sql.rows(sqlexprn,studentSelectParms())
	}
	
	List getEmailAddresses(Integer pidm){
		// return list of email addresses for a particular recipient
		List email_addr = []
		Sql sql = new Sql(db)
		//Banner Student email(s)
		jobparms['08'].each {
			def r = sql.rows(qrySudentBannerEmailAddress,
					[pidm:pidm, mailcode:it])
			r.each { email_addr << it.GOREMAL_EMAIL_ADDRESS }
		}
		//Proxy
		jobparms['09'].each {
			def r = sql.rows(qrySudentProxyEmailAddress,
					[pidm:pidm, proxyrole:it, authfilter:jobparms['10'] ])
			r.each {email_addr << it.gpbprxy_email_address }
		}
		// return unique list
		return email_addr*.toLowerCase()?.unique()
	}
	
	String getParmTermCode(){
		return jobparms['11']
	}
	
	String cvtSelectToCount(String qry){
		String q = 'select 0 count from dual'
		def idx = qry.toLowerCase().indexOf('from')
		if (idx > -1){
			q = "select count(*) count " + qry.substring(idx)
		}
		return q
	}
	
	Integer getStudentSource(){
		//Identify the source of the target students
		//0=Single ID, 1=Population selection, 
		//2=most recent billing recipients, 3=don't know
		
		Integer srcIdx
		if (jobparms['02']) {
			// single student id
			srcIdx = 0
		} else if (jobparms['03'] && jobparms['04']) {
			//via a selection population
			srcIdx = 1
//		} else if (jobparms['07'] == 'Y') { 
//			// from billing population
//			srcIdx = 2
		} else {
			// none to be found
			srcIdx = 3
		}
		return srcIdx
	}
	
	String getStudentSourceName(){
		List srcName = ['SudentID','Population','Billing Recitpients','Unknown']
		return srcName[getStudentSource()]
	}
	
	Map studentSelectParms(){
		// return a map of the parameters used by the qryStudent expressions
		// map names match the query bind variable names
		return [stuId : jobparms['02'] , popsel_appl : jobparms['03'],
		popsel_sel : jobparms['04'], parm_popsel_crID : jobparms['05'],
		parm_popsel_usID : jobparms['06'], 'dummy' : '']
	}
	
	List generateLetter(def stu){
		// Given a student (pidm/id/name/... generate letter
		// Populate a Letter with the input data
		// result[0] count of success, [1]=count of errors, 
		//     [2]=count gurmail entry inserts, [3]=count gurmail entry updates
		// subsequent elements are result messages for each email address.
		List results = [0,0,0,0]
		String mode = jobparms['12']   //A|U
		String verbose =jobparms['07'] //Y|N
		Letter ltr = new Letter([pidm : stu.pidm, id : stu.id,
			fullName : stu.fullname, lastName : stu.last_name,
			firstName : stu.first_name, parmTermCode : getParmTermCode(),
			emailAddresses : getEmailAddresses( (Integer) stu.pidm )])
		setGurmail(ltr) // populate the gurmail row and signature info
		fetchStuDynoData(ltr) // populate dynamic Data for the student
		fetchStuBillingInfo(ltr) // populate billing message if needed
		
		String message_text =  ltr.composeLetter(letterText)
		
		if (verbose == 'Y'){ltr.dump()}

		if (mode == 'A' || verbose == 'Y' ){
			// here in audit mode or verbose.. show what would have sent
			println "Email Start ====================================="
			if (ltr.emailAddresses.size() > 0){
				ltr.emailAddresses.each { 
					println "To: ${ltr.fullName} <${it}>"
					if (mode == 'A' ){
						results[0] += 1
						results << "Email Test (${it}) "
					}
				}
			} else {
				results << "Not Sent - No Email Address"
			}
			String sender_name = jobparms['15']
			if (sender_name){
			  println "From: ${sender_name} <${jobparms['14']}>"
			}else{
			  println "From: ${jobparms['14']}\n"
			}
				
			String subject = jobparms['13']
			if (subject){
				println "Subject: ${subject}"
			}
			println "\n${message_text}\n"
			println "Email End ====================================="
		}

		Boolean bsuccess = false
		Sql sql = new Sql(db)
		if (mode == 'U'){
			// Here sending the emails
			//  mail successful will be T or F 
            // if it is not true to be T, then an error occurred
			def mail_success
			def mail_errmsg
			def l_error_type
            def l_error_code
            def l_error_msg

			if (ltr.emailAddresses.size() > 0){
				ltr.emailAddresses.each {emailaddr ->
					sql.call(execSendEmail, [emailaddr,ltr.fullName,jobparms['14'],
						jobparms['15'],jobparms['16'], jobparms['13'],message_text,
						Sql.inout(Sql.VARCHAR(mail_success)) ,
						Sql.inout(Sql.VARCHAR(mail_errmsg)),
						Sql.inout(Sql.VARCHAR(l_error_type)),
						Sql.inout(Sql.VARCHAR(l_error_code)),
						Sql.inout(Sql.VARCHAR(l_error_msg))
						])  { successflag, errmsg, error_type, error_code, error_msg  ->
								bsuccess = bsuccess || (successflag == 'T')
								results << "${errmsg} (${emailaddr}) "
								results[0] += (successflag == 'T') ? 1 : 0 //increment success count
								results[1] += (successflag != 'T') ? 1 : 0 //increment error count
							}
				}
			} else {
				results << "Not Sent - No Email Address"
			}
		}
		
		if (bsuccess){
			// update gurmail & if any email went.
			if ( ltr.rowid ){
				sql.execute updateGurmail, [mail_row : ltr.rowid]
				results[3] += 1 
			} else {
				// Insert a gurmal record for this letter
				sql.execute insertGurmail, 
					[pidm : ltr.pidm, term_code : getParmTermCode(), system_ind : 'S',
					 letter_module : letter_module, letter_code : letter_code]
				results[2] += 1 
			}
		}
		
		if (verbose  == 'Y'){println "Generate Letter response: " + results}
		return results
	}
	
	void setGurmail(Letter l){
		// plug the gurmail record properties into the letter
		Sql sql = new Sql(db)
		def r = sql.firstRow(qrySudentGurmailRow,
			[pidm:l.pidm, letter_code:letter_code, letter_module:letter_module , term_code: jobparms['11']])
		if (r){
			l.letrCode = r.gurmail_letr_code
			l.termCode = r.gurmail_term_code
			l.adminIdentifier = r.gurmail_admin_identifier
			l.rowid = r.rowid
			l.initCode = r.gurmail_init_code

			//Now set signature stuff..if present
			r = sql.firstRow(qrySudentSignatureRow,[mail_init_code : l.initCode])
			if (r){
				l.sigName = r.stvinit_desc
				l.sigEmailAddress = r.stvinit_email_address
				l.sigTitle1 = r.stvinit_title1
				l.sigTitle2 = r.stvinit_title2
			}
		}
		// make the dynamic query template..to be filled in
		l.qryDynamic = makeDynoExpression(letter_module, l.adminIdentifier )
	}
	
	
	void getDynamicData(Letter l){
		// fetch and attach the dynamic data to be used in the letter
		Sql sql = new Sql(db)
		def r = sql.firstRow(qrySudentGurmailRow,
			[pidm:l.pidm, letter_code:letter_code, letter_module:letter_module , term_code: jobparms['11']])
		if (r){
			l.letrCode = r.gurmail_letr_code
			l.termCode = r.gurmail_term_code
			l.adminIdentifier = r.gurmail_admin_identifier
			l.rowid = r.rowid
			l.initCode = r.gurmail_init_code

			//Now set signature stuff..if present
			r = sql.firstRow(qrySudentSignatureRow,[mail_init_code : l.initCode])
			if (r){
				l.sigName = r.stvinit_desc
				l.sigEmailAddress = r.stvinit_email_address
				l.sigTitle1 = r.stvinit_title1
				l.sigTitle2 = r.stvinit_title2
			}
		}
		// make the dynamic query template..to be filled in
		l.qryDynamic = makeDynoExpression(letter_module, l.adminIdentifier )
	}
	
	void fetchStuBillingInfo(Letter ltr){
		// populate billing message and recipient name if needed
		if (letterUsesBillData){
			Sql sql = new Sql(db)
			def r =  sql.firstRow(qryBillMessage, [pidm : ltr.pidm])
			ltr.letterUsesBillData = letterUsesBillData
			ltr.billInfo = [:]
			Letter.billInfoTags.each{ tag, fldnm->
				ltr.billInfo << [ "${tag}" : (r) ? r."${fldnm}" : '']
			}
//			String m = (r) ? r.BILL_MESSAGE : ''
//			String n = (r) ? r.RECIPIENT_NAME : '' 
//			ltr.billInfo = [ (Letter.billInfoTags[0]) :  m, (Letter.billInfoTags[1]) : n ] 
		}else {
			ltr.letterUsesBillData = letterUsesBillData
			ltr.billInfo = [:] 
		}
	} 

	void dump(){
		// tester method to dump state of this letter service
		
		if ( db ) {
			//Database Connection
			println "\nConnection Metadata\n===================="
			def metad = db.getMetaData()
			println "${metad.getDatabaseProductName()} Version ${metad.getDatabaseProductVersion()}"
			println "${metad.getDriverName()} Version ${metad.getDriverVersion()}"
			//println "\nConnection Properties\n================"
			def p = db.getProperties()  //.each {println it}
			println sp(p, 'autoCommit')
			println sp(p, 'protocol')
			println sp(p, 'v$session.osuser')
			println sp(p, 'oracle.jdbc.commitOption')
			println sp(p, 'user')
			println sp(p, 'v$session.program')
			println sp(p, 'database')
			
		} else {
			println "!!! No Database"
		}
		
		if ( jobparms ) {
			//Database Connection
			println "\nParameter Map\n====================="
			jobparms.sort()*.key.each {println "${it} - ${jobparms[it]}" }
		} else {
			println "!!! No Job Parameters"
		}

		
		// Properties holding letter definition
		// used to generate the emails.
		println "\nLetterCode= ${letter_code} - Module= ${letter_module} - View= ${letter_view}"
		
		if ( letterText ) {
			//Letter Text
			println "\nLetter Text\n====================="
			letterText.each {println "${it}" }
		} else {
			println "!!! No Letter Text"
		}


		if ( letterDynoCols ) {
			println "\nDynamic Columns\n====================="
			letterDynoCols.each {println "${it}" }
		} else {
			println "!!! No Dynamic Columns"
		}
		
		List srcName = ['SudentID','Population','Billing Recitpients','Unknown']
		println "\n${countStudents()} students to email.  Student source is ${getStudentSourceName()}"
		
	}
	
	String sp(Map m, String k){
		//show properties..
		return k + ':' + m[k]
	}
	
	String makeDynoExpression(String ltrmodule, String mailseqno ){
		// The dynamic sql depends on if there is a mailseqno present
		// and the module of the letter
		String exprn = ""
		if (mailseqno && ltrmodule=="A" ) {
			exprn = "select {0} from {1} where pidm_key = :spriden_pidm and term_code_key = :mail_term and appl_no_key =:mail_seqno"
		} else if (mailseqno && ltrmodule=="R"){
			exprn = "select {0} from {1} where pidm_key = :spriden_pidm and term_code_key = :mail_term and recruit_data_seq_no_key =:mail_seqno"
		} else if (ltrmodule=="A") {
			exprn = "select {0} from {1} where pidm_key = :spriden_pidm and term_code_key = :mail_term "
		} else if (ltrmodule=="R") {
			exprn = "select {0} from {1} where pidm_key = :spriden_pidm and term_code_key = :mail_term "
		} else if (ltrmodule=="S") {
		 	exprn ="select {0} from {1} where pidm_key = :spriden_pidm and term_code_key = :parm_term_code "
		}
		return exprn
	}
	
	void fetchStuDynoData(Letter l){
		Sql sql = new Sql(db)
		Map res = [:]
		Map dflt = [:]
		
		letterDynoCols.each {
			// skip the null entry in the query result... fetch data for all columns specified
			if (it.SORELTR_COLUMN_ID){
				String exprn = l.getQryDynamic()
				exprn = exprn.replace('{0}',it.SORELTR_COLUMN_ID).replace('{1}',letter_view)
				dflt = ["${it.SORELTR_COLUMN_ID}" : '']
				//println exprn
				def r = sql.firstRow(exprn, l.getDynoParms())
				res << (r ?: dflt)
			}
		}
		l.dynamicData = res 
	}
	Map fetchJobParmNames(){
		Map r = [:]
		Sql sql = new Sql(db)
		def qres = sql.rows(qryParameterNames, [jobname : jobname]) 
		qres.each {r[it.key] = it.label}
		return r
	}
	
	String formatParameters(){
		//display friendly view of the parameters
		String r = ""
		jobparms.sort()*.key.each {r = r + "${it} - ${jobparm_names[it]}: ${jobparms[it]}\n" }
		return r
	}
	
	//persons to send email to by ID
	private static String qryStudentById = """
		select spriden_pidm pidm, spriden_id id, 
	       substr(spriden_last_name || ', ' || spriden_first_name || 
	         decode(spriden_mi,null,' ','','' ||  spriden_mi), 1,30) fullname,
	       spriden_last_name last_name, spriden_first_name first_name
		from spriden
		where spriden_change_ind is null
		and   spriden_id = :stuId
		order by spriden_last_name, spriden_first_name, spriden_mi,spriden_id
	"""

	//persons to send email to from population
	private static String qryStudentByPop = """
		select spriden_pidm pidm, spriden_id id, 
	       substr(spriden_last_name || ', ' || spriden_first_name || 
	         decode(spriden_mi,null,' ','','' ||  spriden_mi), 1,30) fullname,
	       spriden_last_name last_name, spriden_first_name first_name
		from spriden, glbextr
		where spriden_change_ind is null
		and   spriden_pidm = glbextr_key
		and   glbextr_application = upper(:popsel_appl)
		and   glbextr_selection = upper(:popsel_sel)
		and   glbextr_creator_id = upper(nvl(:parm_popsel_crID, user))
		and   glbextr_user_Id = upper(nvl(:parm_popsel_usID, user))
		order by spriden_last_name, spriden_first_name, spriden_mi,spriden_id
	"""

	//persons to send email from most recent billing recipients
	// added :dummy to get past 'Invalid Column Type' bind error when query has no parameters.. nasty
	private static String qryStudentByBill = """
		select spriden_pidm pidm, spriden_id id, 
	       substr(spriden_last_name || ', ' || spriden_first_name || 
	         decode(spriden_mi,null,' ','','' ||  spriden_mi), 1,30) fullname,
	       spriden_last_name last_name, spriden_first_name first_name
		from spriden, twbpop, twbparm
		where spriden_change_ind is null
		and   spriden_pidm = twbpop_pidm
		and twbpop_bill_date = (select max(twvbill_bill_date) from twvbill)
		and twbparm_bill_date = twbpop_bill_date
		and twbparm_population = twbpop_population
		and '' = :dummy
		order by spriden_last_name, spriden_first_name, spriden_mi,spriden_id
	"""

	//persons to send email from unknown source
	// added :dummy to get past 'Invalid Column Type' bind error when query has no parameters.. nasty
	private static String qryStudentByUnknown = "select 'X' from dual where dummy = :dummy" //Invalid ColumnType when 0 bind parms????
	
	//get student email address of a particular type (if present)
	private static String qryStudentEmailAddr = """
		select goremal_email_address from goremal
		where goremal_pidm = :pidm
		and goremal_emal_code = :emailCode
		and goremal_status_ind = 'A'
	"""

	//get student's proxy(s) email address of a particular role (if present)
	// and the proxy has access granted to a or all ssb screens.
	private static String qryProxyEmailAddr = """
		select gpbprxy_email_address from gprxref, gpbprxy
		where gprxref_person_pidm = :pidm and gprxref_retp_code = :proxy_role
		and gprxref_proxy_idm = gpbprxy_proxy_idm and gpbprxy_pin_disabled_ind = 'N'
		and gpbprxy_pin_exp_date > sysdate
		and exists (select 'x' from gprauth where gprauth_proxy_idm = gprxref_proxy_idm 
			and gprauth_person_pidm = gprxref_person_pidm and gprauth_auth_ind = 'Y' 
			and gprauth_page_name like :pageaccess
	"""
	
	// select the letter text to send
	private static String qryLetterText = """
		select soreltr_text_var ,
			   soreltr_column_id,
			   upper(soreltr_format_var) soreltr_format_var
		from soreltr
		where  soreltr_letr_code = :letter_code
		order by soreltr_seq_no
	"""

	// Select the letter columns with which to parse dynamic sql
	private static String qryLetterDynoCols = """
		select soreltr_column_id
		from soreltr
		where  soreltr_letr_code = :letter_code
		group by soreltr_column_id
		order by soreltr_column_id
	"""

	// Get the view name and source code for the letter
	private static String qryLetterDynoColsView = """
		SELECT stvelmt_view , stvelmt_code
		FROM   stvelmt, soreltl
		where soreltl_letr_code = :letter_code
		and stvelmt_code = soreltl_elmt_code
		and not ( stvelmt_code in ('E', 'P'))
	"""
	
	// Get the signature for the letter
	private static String qrySudentSignatureRow = """
	select stvinit_desc, stvinit_email_address,	stvinit_title1,	stvinit_title2
	from   stvinit 	where  stvinit_code=:mail_init_code
	"""

	// Get Banner email address for student of a particular type
	private static String qrySudentBannerEmailAddress = """
		SELECT GOREMAL_EMAL_CODE, GOREMAL_EMAIL_ADDRESS
		FROM SPRIDEN, GOREMAL
		WHERE SPRIDEN_PIDM = :pidm
		AND SPRIDEN_CHANGE_IND IS NULL
		AND GOREMAL_PIDM = SPRIDEN_PIDM
		AND GOREMAL_STATUS_IND = 'A'
		AND GOREMAL_EMAL_CODE = :mailcode
	"""
	
	// Get Proxy email address for student of a particular type
	private static String qrySudentProxyEmailAddress = """
	select gpbprxy_email_address from gprxref, gpbprxy
	where gprxref_person_pidm = :pidm 
	and gprxref_retp_code = UPPER( :proxyrole)
	and gprxref_proxy_idm = gpbprxy_proxy_idm
	and gpbprxy_pin_disabled_ind = 'N'
	and gpbprxy_pin_exp_date > sysdate
	and exists (select 'x' from gprauth where gprauth_proxy_idm = gprxref_proxy_idm and gprauth_person_pidm = gprxref_person_pidm
		and gprauth_auth_ind = 'Y' and gprauth_page_name like :authfilter )
	"""

	// Send the email
	private static String execSendEmail = """
	BEGIN
	SOKEMAL.P_SENDEMAIL(:email, :spriden_name,
	 :parm_email_address, :parm_sender_name,
	 :parm_email_server,:parm_email_subject,
	 :email_memo, :mail_successful, :mail_errmsg,
     :l_error_type, :l_error_code, :l_error_msg);
   END;
	"""
		
	// Get the student's gurmail row for the letter
	private static String qrySudentGurmailRow = """
	select gurmail_letr_code,
		gurmail_term_code, gurmail_admin_identifier,
		 gurmail.rowid,
		 gurmail_init_code
	  from gurmail
	  where  gurmail_pidm = :pidm
	  and gurmail_letr_code = :letter_code
	  and gurmail_module_code = :letter_module
	   and gurmail_date_printed is null
	  and (( gurmail_term_code = :term_code)
	  or (gurmail_term_code = '999999' and gurmail_module_code = :letter_module))
	"""
	// update Students gurmail w/ the print date
	private static String updateGurmail = """
	set gurmail_date_printed = sysdate,
	gurmail_activity_date = sysdate,
	GURMAIL_ORIG_IND = 'E'
    where rowid = :mail_row
	"""
	// insert a gurmail entry for this student w/ the print date
	private static String insertGurmail = """
       INSERT INTO GURMAIL
        (GURMAIL_PIDM,
         GURMAIL_TERM_CODE,
         GURMAIL_AIDY_CODE,
         GURMAIL_SYSTEM_IND,
         GURMAIL_LETR_CODE,
         GURMAIL_MODULE_CODE,
         GURMAIL_DATE_PRINTED,
         GURMAIL_USER,
         GURMAIL_ACTIVITY_DATE,
         GURMAIL_ORIG_IND)
       VALUES
        (:pidm,
         :term_code,
         uvm_utils.acadyr(:term_code),
         :system_ind,
         :letter_code,
         :letter_module,
         SYSDATE,
         USER,
         SYSDATE,
         'E')
	"""

	private static String qryParameterNames = """
	select gjbpdef_number key, gjbpdef_desc label from gjbpdef
	where gjbpdef_job = :jobname
	order by gjbpdef_number
	"""
	
	private static String qryBillMessage = """
	select RECIPIENT_NAME, BILL_MESSAGE from UVM_BILLING_DATA where pidm_key = :pidm
	"""
	
}

