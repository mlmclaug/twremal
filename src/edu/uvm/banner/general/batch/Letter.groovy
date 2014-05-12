package edu.uvm.banner.general.batch

import java.util.Map;

class Letter {
	Integer pidm
	String id
	String fullName
	String lastName
	String firstName
	String parmTermCode
	// gumail row (if already there)
	String letrCode
	String termCode
	String adminIdentifier
	def rowid
	String initCode
	
	// Signature row
	String sigName
	String sigEmailAddress
	String sigTitle1
	String sigTitle2
	
	//Recipient emailAddresses
	List emailAddresses = []
	
	//Dynamic query express template
	String qryDynamic
	//Map of the dynamic data to be inserted into the letter text
	Map dynamicData = [:]
	Map formatTranslates = [ "<BR>":"\n","<H>":"\n","<P>":"\n\t"]
	// Initially add a column to the AS_STUDENT_DATA view for the billing message. 
	// Unfortunately AS is 'active students' not 'all students' and the billing recipients
	// include many who are not active students... to get around this changed to 
	// insert the message as a text substitution of a keyword: ${BILL_MESSAGE}
	Boolean letterUsesBillData // include billing information in email
	static Map billInfoTags = ['${BILL_MESSAGE}':'BILL_MESSAGE','${BILL_RECIPIENT}': 'RECIPIENT_NAME']
	Map billInfo = [:]
	
	Map getDynoParms(){
		//Return map w/ bind parameters used in the dynamic queries
		return [spriden_pidm : pidm,
			mail_term : termCode,
			mail_seqno :  adminIdentifier,
			parm_term_code : parmTermCode]
	}
	static Boolean hasTag(String txt){
		// return true if text contains any tag.
		if (txt){
		 billInfoTags.any {k,v -> txt?.contains(k)}
		}
	}

	String composeLetter(def letterText){
		//Composes the body of the email to be sent
		List body = []

		letterText.each {e ->
			String text_var = e.soreltr_text_var 
			String column_id = e.soreltr_column_id
			String format_var  = e.soreltr_format_var

			String dd = column_id ? dynamicData[column_id] : ''
			
			if (letterUsesBillData ){
				billInfo.each { tag, fldnm ->
					// Search for use of tag.  If found do the replace
					if (text_var?.contains(tag)){
						text_var = text_var.replace(tag, billInfo.get(tag))
					}
				}
			}
			
			if ( format_var == null || (dd?.size()>0 && text_var?.size()>0)){
				;
			}else{
				// insert line breaks if present
				body << formatTranslates[format_var]
			}

			if (column_id){
				//Here have dynamic data to insert
				body << (dd ?: '')
			}

			if (text_var){
				// insert text (w/ trailing space)
				body << text_var + ((0<text_var.size()) ? ' ' : '')
			}
		}
		
		// Add signature info if present.
		if (sigName != null){ body << "\n\n${sigName}" }
		if (sigTitle1 != null){ body << "\n${sigTitle1}" }
		if (sigTitle2 != null){ body << "\n${sigTitle2}" }
		if (sigEmailAddress != null){ body << "\n${sigEmailAddress}" }
		
		return body.join()
	}

	
	void dump(){
		// tester method to dump state of this letter service
		println 'Letter'
		println '======'
		println "Student - pidm=${pidm}, id=${id}, name=${fullName}, parmTermCode=${parmTermCode}"
		println "Gurmail - letrCode=${letrCode}, termCode=${termCode}, adminIdentifier=${adminIdentifier}, rowid=${rowid}, initCode=${initCode}"
		println "Signature - sigName=${sigName}, sigEmailAddress=${sigEmailAddress}, sigTitle1=${sigTitle1}, sigTitle2=${sigTitle2}"
		
		println 'Recipient emailAddresses'
		emailAddresses.each { println " + ${it}" }
		
		println "Dynamic query express template - ${qryDynamic}"

		println "Dynamic data to be inserted into the letter text:"
		dynamicData.each { k,v -> println " > ${k}=${v}" }
		
		println "Include Billing Message: ${letterUsesBillData}"
		billInfo.each { k,v -> println " > ${k}=${v}" }
				
	}
}
