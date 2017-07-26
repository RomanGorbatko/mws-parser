<?php
require_once('../../../../includes.php'); 

include_once ('.config.inc.php'); 
$result = mysql_query("SELECT report_id FROM fulfillments WHERE id=1");
$row = mysql_fetch_array($result);
define ('REPORT_ID', $row['report_id']);

class GetReport{
	public function insertReport($array)	{
		//INTAILIZATION
		global $sql_obj;
		$fba_fee 		= 	$array['21']+$array['22'];
		$trans_date		= 	str_replace('T', ' ', $array['6']);
		$trans_date		= 	str_replace('+00:00', '', $trans_date);
		
		
		//CHECK IF ORDER EXIST IN MULTIPLE TIME
		$count	=	$sql_obj->RowCount("fulfillment_detail","WHERE order_id = '".$array['0']."'");
		if($count > 0)	{
			$sql_obj->Query("UPDATE fulfillment_detail SET
							sales_price	 =  sales_price + '".$array['17']."',
							shipping_fee = shipping_fee + '".$array['19']."'
							WHERE order_id = '".$array['0']."'");
		}else {
			
			$sql_obj->Query("INSERT INTO fulfillment_detail ( 
								product_name, 
								transaction_date, 
								order_id, 
								sales_price, 
								shipping_fee, 
								sales_tax,
								sales_tax_service_fee, 
								fba_fee
							) VALUES ( 
								'".$array['14']."', 
								'$trans_date', 
								'".$array['0']."', 
								'".$array['17']."', 
								'".$array['19']."', 
								'".$array['20']."', 
								'".$array['20']."', 
								'$fba_fee' 
						)");
		}
	}
	public function getExpences() {
		global $mws_service_array;
		try {
			$parameters = array (
			   'Merchant' => '_GET_FLAT_FILE_PAYMENT_SETTLEMENT_DATA_',
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
             //mysql_query("TRUNCATE TABLE fulfillment_detail");
			 $contents = stream_get_contents($request->getReport());
			 $acontents = explode("\n", $contents);
			 print_r($acontents);
			 die();
			print_r($acontents[0]);
			for($i=29; $i< 31; $i++){	 
			//for($i=1; $i<count($acontents); $i++){
				print_r($acontents[$i]);
				$array	=	explode("\t", $acontents[$i]);
				$this->insertReport($array);
				
				
				
				
				//$trans_date= str_replace('T', ' ', $st['6']);
				//$trans_date= str_replace('+00:00', '', $trans_date);
				//$fba_fee = $st['21']+$st['22'];
				//$sql_obj->Query("insert into inventory_health(product_details)values('".$acontents[$i]."')") or die(mysql_error());
				//mysql_query ("INSERT INTO fulfillment_detail ( product_name, transaction_date, order_id, sales_price, shipping_fee, sales_tax,  shipping_fee_expense, sales_tax_service_fee, fba_fee) VALUES ( '".$st['14']."', '$trans_date', '".$st['0']."', '".$st['17']."', '".$st['19']."', '".$st['20']."', '".$st['19']."', '".$st['20']."', '$fba_fee' )") or die(mysql_error());
				
			}
			//print_r($array_two);
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
             mysql_query("TRUNCATE TABLE fulfillment_detail");
			 $contents = stream_get_contents($request->getReport());
			 $acontents = explode("\n", $contents);
			 //print_r($acontents);
			print_r($acontents[0]);
			for($i=29; $i< 31; $i++){	 
			//for($i=1; $i<count($acontents); $i++){
				print_r($acontents[$i]);
				$array	=	explode("\t", $acontents[$i]);
				$this->insertReport($array);
				
				
				
				
				//$trans_date= str_replace('T', ' ', $st['6']);
				//$trans_date= str_replace('+00:00', '', $trans_date);
				//$fba_fee = $st['21']+$st['22'];
				//$sql_obj->Query("insert into inventory_health(product_details)values('".$acontents[$i]."')") or die(mysql_error());
				//mysql_query ("INSERT INTO fulfillment_detail ( product_name, transaction_date, order_id, sales_price, shipping_fee, sales_tax,  shipping_fee_expense, sales_tax_service_fee, fba_fee) VALUES ( '".$st['14']."', '$trans_date', '".$st['0']."', '".$st['17']."', '".$st['19']."', '".$st['20']."', '".$st['19']."', '".$st['20']."', '$fba_fee' )") or die(mysql_error());
				
			}
			//print_r($array_two);
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
				 //$response = $mws_service_array->getReport($request);
				  $response = $service->getReportList($request);
				  print_r($response);
				  
				   
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

$report->invokeGetReportList();

//$request->invokeGetReportList();
                                                                               
