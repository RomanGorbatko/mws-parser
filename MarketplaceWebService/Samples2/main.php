<?php
require_once('../../../../includes.php'); 

include_once ('.config.inc.php'); 
$result = mysql_query("SELECT report_id FROM `reports` WHERE id=1");
$row = mysql_fetch_array($result);
define ('REPORT_ID', $row['report_id']);

class GetReport{
	public function getinventoryHealthReport() {
		global $mws_service_array;
		try {
			$parameters = array (
			   'Merchant' => MERCHANT_ID,
			   'Report' => @fopen('php://memory', 'rw+'),
			   'ReportId' => REPORT_ID,
			   'MWSAuthToken' => '<MWS Auth Token>', // Optional
			 );
			 
			 $request = new MarketplaceWebService_Model_GetReportRequest($parameters);
			 $request = new MarketplaceWebService_Model_GetReportRequest();
			 $request->setMerchant(MERCHANT_ID);
			 $request->setReport(@fopen('php://memory', 'rw+'));
			 $request->setReportId(REPORT_ID);
             $response = $mws_service_array->getReport($request);
			 echo "<pre>";
             mysql_query("TRUNCATE TABLE inventory_health_detail");
			 $contents = stream_get_contents($request->getReport());
			 $acontents = explode("\n", $contents);
			 //print_r($acontents);
				
				 
			for($i=0; $i<count($acontents); $i++){
				$st= explode("\t", $acontents[$i]);
				
				//print_r ($st);
				
				if($i>0){	
				
				//$sql_obj->Query("insert into inventory_health(product_details)values('".$acontents[$i]."')") or die(mysql_error());
				mysql_query("INSERT INTO `inventory_health_detail` ( `asin`, `sku`, `sellable-quantity`, `sales-price`, `units-shipped-last-30-days`) VALUES ( '".$st['3']."', '".$st['1']."', '".$st['9']."', '".$st['31']."', '".$st['18']."' )") or die(mysql_error());
				
				}
			}
				
				die('ok');
				
                echo("            ResponseHeaderMetadata: " . $response->getResponseHeaderMetadata() . "\n");
		 } catch (MarketplaceWebService_Exception $ex) {
			 echo("Caught Exception: " . $ex->getMessage() . "\n");
			 echo("Response Status Code: " . $ex->getStatusCode() . "\n");
			 echo("Error Code: " . $ex->getErrorCode() . "\n");
			 echo("Error Type: " . $ex->getErrorType() . "\n");
			 echo("Request ID: " . $ex->getRequestId() . "\n");
			 echo("XML: " . $ex->getXML() . "\n");
			 echo("ResponseHeaderMetadata: " . $ex->getResponseHeaderMetadata() . "\n");
		 }
 	}
	public function invokeGetReportList() 
	  {
		  global $mws_service_array;
		  try {
				 $request = new MarketplaceWebService_Model_GetReportListRequest();
				 $request->setMerchant(MERCHANT_ID);
				 $request->setAvailableToDate(new DateTime('now', new DateTimeZone('UTC')));
				 $request->setAvailableFromDate(new DateTime('-3 months', new DateTimeZone('UTC')));
				 $request->setAcknowledged(false);
				 $response = $mws_service_array->getReport($request);
				  //$response = $service->getReportList($request);
				  
				   
					if ($response->isSetGetReportListResult()) { 
					   
						$getReportListResult = $response->getGetReportListResult();
						
						
						$reportInfoList = $getReportListResult->getReportInfoList();
						foreach ($reportInfoList as $reportInfo) {
							
								echo("                        " . $reportInfo->getReportId() . "\n");
							
							
								echo("                        " . $reportInfo->getReportType() . "\n");
						   
							
							
								echo("                        " . $reportInfo->getAvailableDate()->format(DATE_FORMAT) . "\n");
							
							
							
						}
					} 
					
	
					
		 } catch (MarketplaceWebService_Exception $ex) {
			 echo("Caught Exception: " . $ex->getMessage() . "\n");
			 echo("Response Status Code: " . $ex->getStatusCode() . "\n");
			 echo("Error Code: " . $ex->getErrorCode() . "\n");
			 echo("Error Type: " . $ex->getErrorType() . "\n");
			 echo("Request ID: " . $ex->getRequestId() . "\n");
			 echo("XML: " . $ex->getXML() . "\n");
			 echo("ResponseHeaderMetadata: " . $ex->getResponseHeaderMetadata() . "\n");
		 }
	 }
}
$report= new GetReport();

$report->getinventoryHealthReport();

//$request->invokeGetReportList();
                                                                               
