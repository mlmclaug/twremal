package edu.uvm.banner.general.batch
import java.sql.Connection
import java.sql.SQLException
import java.util.Map;

import com.sct.messaging.bif.BatchProcessorException
import com.sct.messaging.bif.BatchResourceHolder
import com.sct.messaging.bif.banner.BannerBatchProcessor
import com.sct.messaging.bif.banner.BannerUtils
//import org.apache.log4j.Logger
import groovy.sql.Sql
import edu.uvm.banner.general.reporter.TabularReport

class Twremal  extends BannerBatchProcessor{
	//private static Logger logger = Logger.getLogger(Twphbif.class)
	String release = '1.0'
	public void processJob() throws BatchProcessorException {
		println "Start process ${getJobName()} using one_up = ${getJobNumber()}" 
		Connection conn = BatchResourceHolder.getConnection()
		Map<String,String> parameterMap = BatchResourceHolder.getJobParameterMap()
		TabularReport rpt = initReport(parameterMap['01'])
		
		// initialize LetterService, setup the letter, count students to process
		LetterService ltrsvc = new LetterService(db : conn, jobparms : parameterMap, jobname : getJobName())
		String ltrErrMsg = ltrsvc.initLetter()	
		def numStudents = ltrsvc.countStudents()
		Integer emailsSent = 0
		Integer emailsError = 0
		Integer numStudentswoEmails = 0
		Integer gurmailMissing = 0
		
		if (ltrErrMsg || numStudents == 0){
			// Here have nothing to process.. print error(s) to console & report and exit.
			if (ltrErrMsg) {println ltrErrMsg; rpt.pl(ltrErrMsg)}	
			if (numStudents == 0) {
					String errmsg = '!!! No Students meet the selection criteria for ' + ltrsvc.getStudentSourceName()
					println(errmsg); rpt.pl(errmsg)
				}	
		} else {
			//process each student
			if (parameterMap['07'] == 'Y'){ltrsvc.dump()}
			def students = ltrsvc.getStudents()
			students.each { stu ->
				println "Processing ${stu}"
				List response = ltrsvc.generateLetter(stu)
				//increment control total counts
				emailsSent += response[0]
				emailsError += response[1]
				gurmailMissing += response[2]
				numStudentswoEmails += (response[0]+response[1] == 0) ? 1 : 0 
	
				rpt.pl([stu.id, stu.fullname, response[3]])
				response[4..response.size()-1].each {rpt.pl(['', '', it]) }
				//commit any updates.
				try {
					BannerUtils.performCommit(conn)
				} catch (SQLException sqle) {
					String errmsg = "!!! Commit failed on ${stu}"
					BannerUtils.performRollback(conn)
				}
			}
		}
		
		// display sorted parameter list & processed counts
		//rpt.newPage()
		rpt.pl('')
		rpt.pl("* * * REPORT CONTROL INFORMATION - ${getJobName()} - Release ${release} * * *")
		rpt.pl('')
		rpt.pl(ltrsvc.formatParameters())
		rpt.pl('')
		rpt.pl("Number of Students Read: ${numStudents}")
		rpt.pl("Number of Emails Sent: ${emailsSent}")
		rpt.pl("Number of Emails Error: ${emailsError}")
		rpt.pl("Number Students with no Email: ${numStudentswoEmails}")
		rpt.pl("Number missing GURMAIL record for letter: ${gurmailMissing}")
		//rpt.pl("Number of persons missing ")
		
		try {
			BannerUtils.performCommit(conn)
		} catch (SQLException sqle) {
			BannerUtils.performRollback(conn)
		}
	}
	
	String makelisfilename(){
		// make a path & file name for the report output file.
		String fs = System.getProperty('file.separator')
		String path = System.getProperty('lis.file.path')
		path = path ? path + fs : ""
		String filename = getJobName() + "_" + getJobNumber() + '.lis'
		filename = filename.toLowerCase()
		return path + filename
	}
	TabularReport initReport(String letter_code){
		TabularReport r = new TabularReport(lpp : 51, cpl : 80, outputDest : new File(makelisfilename()) )
		r.addHead(new Date().format('MM/dd/yyyy h:mm a'), "University of Vermont",{"Page " + delegate.pgno})
		.addHead("", "Student Email","TWREMAL")
		.addHead("Email Letter Code: ${letter_code}", "","")
		.addColHead( 10, 'L',"%-10s", ["ID"])
		.addColHead( 25, 'L',"%-25s",  ["Name"])
		.addColHead( 45, 'L',"%-45s", ["Email Status"])
		return r
	}

}
