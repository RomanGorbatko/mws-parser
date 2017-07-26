<?php
set_time_limit(1000);
include_once ('.config.inc.php'); 
/*$serviceUrl = "https://mws.amazonservices.com";
$config = array (
  'ServiceURL' => $serviceUrl,
  'ProxyHost' => null,
  'ProxyPort' => -1,
  'MaxErrorRetry' => 3,
);
$service = new MarketplaceWebService_Client(
     AWS_API_KEY, 
     AWS_API_SECRET_KEY, 
     $config,
     APPLICATION_NAME,
     APPLICATION_VERSION);*/

class Reporting	{
	private				$service;
	private				$aws_api_key;
	private				$aws_secret_key;
	private 			$serviceUrl = "https://mws.amazonservices.com";
	private				$marchent_id;
	private				$marketplace_id;
	public function __construct($am_credentials)	{
	
		$this->aws_api_key			=	$am_credentials['AWS_API_KEY'];
		$this->aws_secret_key		=	$am_credentials['AWS_API_SECRET_KEY'];
		$this->marchent_id			=	$am_credentials['MERCHANT_ID'];
		$this->marketplace_id		=	$am_credentials['MAKETPLACE_ID'];
		try	{
			$config = array (
			  'ServiceURL' => $this->serviceUrl,
			  'ProxyHost' => null,
			  'ProxyPort' => -1,
			  'MaxErrorRetry' => 3,
			);
			$this->service = new MarketplaceWebService_Client(
							 $am_credentials['AWS_API_KEY'], 
							 $am_credentials['AWS_API_SECRET_KEY'], 
							 $config,
							 APPLICATION_NAME,
							 APPLICATION_VERSION);
		}catch(Exception $e)	{
			echo $e->getMessage();
			die();
		}
	}
	public function invokeGetReportList($report_type) {
		$service = $this->service;
		
		$request = new MarketplaceWebService_Model_GetReportListRequest();
		$request->setMerchant($this->marchent_id);
		/////////////////////////
		$report_type_array = new MarketplaceWebService_Model_TypeList();
		$report_type_array->setType($report_type);
		$request->setReportTypeList($report_type_array);
		////////////////////////
		$request->setAvailableToDate(new DateTime('now', new DateTimeZone('UTC')));
		$request->setAvailableFromDate(new DateTime('-3 months', new DateTimeZone('UTC')));
		$request->setAcknowledged(false);
		try {
			$response = $service->getReportList($request);
			if ($response->isSetGetReportListResult()) {
				$getReportListResult = $response->getGetReportListResult();
				$reportInfoList = $getReportListResult->getReportInfoList();
				foreach ($reportInfoList as $reportInfo) {
					$report_id = $reportInfo->getReportId();
					return $report_id;  			
				 }
			  }else	{
				  return false;
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
	public function invokeRequestReport($report_type) {
		$service = $this->service;
		$marketplaceIdArray = array("Id" => array($this->marketplace_id));
		$parameters = array (
		   'Merchant' => $this->marchent_id,
		   'MarketplaceIdList' => $marketplaceIdArray,
		   'ReportType' => $report_type,
		   'ReportOptions' => 'ShowSalesChannel=true',
		   //'MWSAuthToken' => '<MWS Auth Token>', // Optional
		 );
		 $request = new MarketplaceWebService_Model_RequestReportRequest($parameters);
		 $request = new MarketplaceWebService_Model_RequestReportRequest();
		 $request->setMarketplaceIdList($marketplaceIdArray);
		 $request->setMerchant($this->marchent_id);
		 $request->setReportType($report_type);
		 try {
			 $response = $service->requestReport($request);
			 if ($response->isSetRequestReportResult()) { 
				  $requestReportResult = $response->getRequestReportResult();
				  if ($requestReportResult->isSetReportRequestInfo()) {
					  $reportRequestInfo = $requestReportResult->getReportRequestInfo();
					  return $reportRequestInfo->getReportRequestId();					
				  }else {
					  return false;
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
	public function UpdateInventory($report_id,$user_id)	{
		//INTAILIZATION
		global $sql_obj;
		$acontents	=	$this->getReport($report_id);
		
		//TRUNCATE TABLE
		$sql_obj->Query("DELETE FROM user_inventory WHERE user_id = '$user_id'");

		//echo "<pre>";
		//print_r($acontents);
		//die();
		for($i=1; $i<count($acontents); $i++){
			$array	=	explode("\t", $acontents[$i]);
			if($array[0] != "" && $array[1] != "")	{
				$sql_obj->Query("INSERT INTO user_inventory (sku, asin, price, quantity, user_id) VALUES ('".$array[0]."', '".$array[1]."', '".$array[2]."', '".$array[3]."', '".$user_id."');");
			}
			//echo "<pre>";
			//print_r($array);
			
			
		}
	}
	public function saveTheOrdersData($report_id)	{
		//INTAILIZATION
		global $sql_obj;
		$acontents	=	$this->getReport($report_id);
		
		//TRUNCATE TABLE
		$sql_obj->Query("TRUNCATE TABLE sale_orders");
		//echo $report_id;
		//die();
		//echo "<pre>";
		//print_r($acontents);
		//die();
		for($i=1; $i<count($acontents); $i++){
		//print_r($acontents[0]);
		//for($i=29; $i< 31; $i++){
			//print_r($acontents[$i]);
			$array	=	explode("\t", $acontents[$i]);
			
			//INTAILIZATION
			$fba_fee 		= 	$array['21']+$array['22'];
			$trans_date		= 	str_replace('T', ' ', $array['6']);
			$trans_date		= 	str_replace('+00:00', '', $trans_date);
			
			
			//CHECK IF ORDER EXIST IN MULTIPLE TIME
			$count	=	$sql_obj->RowCount("sale_orders","WHERE order_id = '".$array['0']."'");
			if($count > 0)	{
				$sql_obj->Query("UPDATE sale_orders SET
								sales_price	 =  sales_price + '".$array['17']."',
								shipping_fee = shipping_fee + '".$array['19']."'
								WHERE order_id = '".$array['0']."'");
			}else {
				
				$sql_obj->Query("INSERT INTO sale_orders ( 
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
	}
	

	public function updateOrdersData($report_id)	{
		
		
		//INTAILIZATION
		global $sql_obj;
		$acontents					=	$this->getReport($report_id);
		
		//UPDATE TABLE BEFORE UPDATE LIVE
			//$sql_obj->Query("UPDATE sale_orders SET 
									//FBAPerOrderFulfillmentFee =  0,
									//FBAPerUnitFulfillmentFee  =  0,
									//FBAWeightBasedFee 		  =  0,
									//Commission				  =  0,
									//ShippingChargeback 		  =  0
						// ");
		//echo "<pre>";
		//print_r($acontents);
		//die();
		for($i=1; $i<count($acontents); $i++){
		//for($i=29; $i< 31; $i++){
			$array	=	explode("\t", $acontents[$i]);
			//CHECK IF ORDER EXIST IN MULTIPLE TIME
			$count	=	$sql_obj->RowCount("sale_orders","WHERE order_id = '".$array[7]."'");
			//print_r($array);

						 
			if($count > 0)	{
				
				switch($array[25])	{
					case "FBAPerOrderFulfillmentFee":
						$sql_obj->Query("UPDATE sale_orders 
									SET FBAPerOrderFulfillmentFee =  FBAPerOrderFulfillmentFee + '".$array[26]."'
									 WHERE order_id = '".$array[7]."'
						 ");
					 	break;
					case "FBAPerUnitFulfillmentFee":
						$sql_obj->Query("UPDATE sale_orders 
									SET FBAPerUnitFulfillmentFee =  FBAPerUnitFulfillmentFee + '".$array[26]."'
									 WHERE order_id = '".$array[7]."'
						 ");
					 	break;
					case "FBAWeightBasedFee":
						$sql_obj->Query("UPDATE sale_orders 
									SET FBAWeightBasedFee =  FBAWeightBasedFee + '".$array[26]."'
									 WHERE order_id = '".$array[7]."'
						 ");
					 	break;
					case "Commission":
						$sql_obj->Query("UPDATE sale_orders 
									SET Commission =  Commission + '".$array[26]."'
									 WHERE order_id = '".$array[7]."'
						 ");
					 	break;
					case "ShippingChargeback":
						$sql_obj->Query("UPDATE sale_orders 
									SET ShippingChargeback =  ShippingChargeback + '".$array[26]."'
									 WHERE order_id = '".$array[7]."'
						 ");
					 	break;
				}
			}
		}
	}
	public function getReport($report_id) {
		
		$mws_service_array =	$this->service;;
		try {
			$parameters = array (
			   'Merchant' => $this->marchent_id,
			   'Report' => @fopen('php://memory', 'rw+'),
			   'ReportId' => $report_id,
			   'MWSAuthToken' => '<MWS Auth Token>', // Optional
			 );
			 
			 $request = new MarketplaceWebService_Model_GetReportRequest($parameters);
			 $request = new MarketplaceWebService_Model_GetReportRequest();
			 $request->setMerchant($this->marchent_id);
			 $request->setReport(@fopen('php://memory', 'rw+'));
			 $request->setReportId($report_id);
             $response = $mws_service_array->getReport($request);
			 $contents = stream_get_contents($request->getReport());
			 $acontents = explode("\n", $contents);
			 return $acontents;
			 
			 // echo "<pre>";
			 //print_r($acontents);
			 //die();
			//print_r($acontents[0]);
			//for($i=29; $i< 31; $i++){	 
			//for($i=1; $i<count($acontents); $i++){
				//print_r($acontents[$i]);
				//$array	=	explode("\t", $acontents[$i]);
				
				//$trans_date= str_replace('T', ' ', $st['6']);
				//$trans_date= str_replace('+00:00', '', $trans_date);
				//$fba_fee = $st['21']+$st['22'];
				//$sql_obj->Query("insert into inventory_health(product_details)values('".$acontents[$i]."')") or die(mysql_error());
				//mysql_query ("INSERT INTO fulfillment_detail ( product_name, transaction_date, order_id, sales_price, shipping_fee, sales_tax,  shipping_fee_expense, sales_tax_service_fee, fba_fee) VALUES ( '".$st['14']."', '$trans_date', '".$st['0']."', '".$st['17']."', '".$st['19']."', '".$st['20']."', '".$st['19']."', '".$st['20']."', '$fba_fee' )") or die(mysql_error());
				
			//}
			//print_r($array_two);
			//	die('ok');
				
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
}



?>
