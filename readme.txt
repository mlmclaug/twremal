This program requires the following to be on the classpath:
  $BANNER_HOME/general/java/gurjbif.jar - Banner's Batch IntegrationFramework
  ojdbc6.jar  - Oracles JDBC driver
  groovy-all-2.0.7.jar  - some version of the groovy-all
  tabularreport.jar - report helper


The main class is: com.sct.messaging.bif.banner.BannerBatchProcessor

Sample Program Arguments: 
mlm jdbc:oracle:thin:@ldap://ldap.uvm.edu:389/AIST,CN=OracleContext,dc=uvm,dc=edu 3061683 TWREMAL

Sample VM Arguments:
-Xms64m -Xmx1024m -Dbatch.processor.class=edu.uvm.banner.general.batch.Twremal -Dlis.file.path=/Users/mlm/dev/noremal/twremal/output