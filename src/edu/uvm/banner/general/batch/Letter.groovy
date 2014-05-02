package edu.uvm.banner.general.batch

import java.util.Map;

class Letter {
	Integer pidm
	String id
	String fullName
	String lastName
	String firstName
	String parmTermCode
	// gumail row
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
	

	Map getDynoParms(){
		//Return map w/ bind parameters used in the dynamic queries
		return [spriden_pidm : pidm,
			mail_term : termCode,
			mail_seqno :  adminIdentifier,
			parm_term_code : parmTermCode]
	}
	

	String composeLetter(def letterText){
		//Composes the body of the email to be sent
		List body = []

		letterText.each {e ->
			String text_var = e.soreltr_text_var 
			String column_id = e.soreltr_column_id
			String format_var  = e.soreltr_format_var

			String dd = column_id ? dynamicData[column_id] : ''
			
			if ( format_var == null || (dd?.size()>0 && text_var?.size()>0)){
				;
			}else{
				// insert line breaks if present
				body << formatTranslates[format_var]
			}

			if (column_id){
				//Here have dynamic data to insert  (w/ trailing space)
				body << dd //+ ((0<dd.size()) ? ' ' : '')
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
				
	}
}
