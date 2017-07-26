<?php
set_time_limit(300);
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
	public function updateHealthInventory($report_id) {
		
		$mws_service_array =	$this->service;
		try {
			$parameters = array (
			   'Merchant' => $this->marchent_id,
			   'Report' => @fopen('php://memory', 'rw+'),
			   'ReportId' => $report_id,
			   //'MWSAuthToken' => '<MWS Auth Token>', // Optional
			 );
			 
			 $request = new MarketplaceWebService_Model_GetReportRequest($parameters);
			 $request = new MarketplaceWebService_Model_GetReportRequest();
			 $request->setMerchant($this->marchent_id);
			 $request->setReport(@fopen('php://memory', 'rw+'));
			 $request->setReportId($report_id);
             $response = $mws_service_array->getReport($request);
			 //echo "<pre>";
             mysql_query("TRUNCATE TABLE inventory_health");
			 $contents = stream_get_contents($request->getReport());
			 $acontents = explode("\n", $contents);
			 //print_r($acontents);
			 //die();	
				 
			for($i=1; $i<count($acontents); $i++){
				$st= explode("\t", $acontents[$i]);
				//echo "INSERT INTO inventory_health (product_name, asin, sku, sellable-quantity, sales-price, units-shipped-last-30-days) VALUES ('".stripslashes($st['4'])."',  '".$st['3']."', '".$st['1']."', '".$st['9']."', '".$st['31']."', '".$st['18']."' )"."<br>";
				//die();
				//print_r ($st);
				mysql_query("INSERT INTO inventory_health (product_name, asin, sku, sellable_quantity, sales_price, units_shipped_last_30_days) VALUES ('".stripslashes($st['4'])."',  '".$st['3']."', '".$st['1']."', '".$st['9']."', '".$st['31']."', '".$st['18']."' )") or die(mysql_error());

			}
           // echo("            ResponseHeaderMetadata: " . $response->getResponseHeaderMetadata() . "\n");
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
		//echo "<pre>";
		///echo $array[3];
		//print_r($acontents);
		//die();

		//print_r($acontents);
		for($i=1; $i<count($acontents); $i++){
			$array	=	explode("\t", $acontents[$i]);
			//print_r($array);
			//die();
			if(isset($array[16]))	{
				
				//print_r($array[16]."---".$array[3]."<br/>");
				$pro_row	=	$sql_obj->RowCount("products","WHERE asin = '".trim($array[16])."' AND sku = '".trim($array[3])."'");
				//echo $pro_row."<br>;";
				$short_pro_name	=	explode('-',$array[0]);
				if((int)$pro_row > 0)	{
					$sql_obj->Query("UPDATE products SET  price = '".$array[4]."', quantity = '".$array[5]."', item_name = '".$array[0]."', item_description = '".$array[1]."', listing_id = '".$array[2]."', open_date = '".$array[6]."', image_url = '".$array[7]."', item_is_marketplace = '".$array[8]."', product_id_type = '".$array[9]."', zshop_shipping_fee = '".$array[10]."', item_note = '".$array[11]."', item_condition = '".$array[12]."', zshop_category1 = '".$array[13]."', zshop_browse_path  = '".$array[14]."', zshop_storefront_feature = '".$array[15]."', asin1 = '".$array[16]."', asin2 = '".$array[17]."', asin3 = '".$array[18]."', will_ship_internationally =  '".$array[19]."', expedited_shipping  ='".$array[20]."', zshop_boldface = '".$array[21]."', product_id  ='".$array[22]."', bid_for_featured_placement  ='".$array[23]."', add_delete = '".$array[24]."', pending_quantity  ='".$array[25]."', fulfillment_channel = '".$array[26]."' WHERE sku = '".trim($array[3])."' AND asin = '".trim($array[16])."' AND user_id = '$user_id'");
					echo "ASIN=".$array[16]." sku= ".$array[3]."--Products is updated<br/>";
				}else {
					$sql_obj->Query("INSERT INTO products (sku, asin, price, quantity, item_name, item_description, listing_id, open_date, image_url, item_is_marketplace, product_id_type, zshop_shipping_fee, item_note, item_condition, zshop_category1, zshop_browse_path, zshop_storefront_feature, asin1, asin2, asin3, will_ship_internationally, expedited_shipping, zshop_boldface, product_id, bid_for_featured_placement, add_delete, pending_quantity, fulfillment_channel, user_id,name) VALUES ('".trim($array[3])."', '".trim($array[16])."', '".$array[4]."', '".$array[5]."', '".$array[0]."', '".$array[1]."', '".$array[2]."', '".$array[6]."', '".$array[7]."', '".$array[8]."', '".$array[9]."', '".$array[10]."', '".$array[11]."', '".$array[11]."', '".$array[13]."', '".$array[14]."', '".$array[15]."', '".$array[16]."', '".$array[17]."', '".$array[18]."', '".$array[19]."', '".$array[20]."', '".$array[21]."', '".$array[22]."', '".$array[23]."', '".$array[24]."', '".$array[25]."', '".$array[26]."', '$user_id','".$short_pro_name[0]."')");
						echo "ASIN=".$array[16]." sku= ".$array[3]."--Products is inserted<br/>";
				}
			}
			
			
			
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
		//
		////////////////////////
		$request->setAvailableToDate(new DateTime('now', new DateTimeZone('UTC')));
		$request->setAvailableFromDate(new DateTime('-3 months', new DateTimeZone('UTC')));
		$request->setAcknowledged(false);
		$reports	=	array();
		$i			=	0;
		try {
			
              $response = $service->getReportList($request);
                if ($response->isSetGetReportListResult()) { 
                  
                    $getReportListResult = $response->getGetReportListResult();
                    if ($getReportListResult->isSetNextToken()) 
                    {
						$report_info['next_token']	=	$getReportListResult->getNextToken();
                       /* echo("                NextToken\n");
                        echo("                    " . $getReportListResult->getNextToken() . "\n");*/
                    }
                    if ($getReportListResult->isSetHasNext()) 
                    {
						$report_info['get_has_next']	=	$getReportListResult->getHasNext();
                       /* echo("                HasNext\n");
                        echo("                    " . $getReportListResult->getHasNext() . "\n");*/
                    }
                    $reportInfoList = $getReportListResult->getReportInfoList();
					$i			=	0;
                    foreach ($reportInfoList as $reportInfo) {
                        if ($reportInfo->isSetReportId()) 
                        {
							$report_info[$i]['report_id']	=	 $reportInfo->getReportId();
                           /* echo("                    ReportId\n");
                            echo("                        " . $reportInfo->getReportId() . "\n");*/
                        }
                        if ($reportInfo->isSetReportType()) 
                        {
							$report_info[$i]['type']	=	$reportInfo->getReportType();
                           /* echo("                    ReportType\n");
                            echo("                        " . $reportInfo->getReportType() . "\n");*/
                        }
                        if ($reportInfo->isSetReportRequestId()) 
                        {
							$report_info[$i]['req_id']	=	$reportInfo->getReportRequestId();
                           /* echo("                    ReportRequestId\n");
                            echo("                        " . $reportInfo->getReportRequestId() . "\n");*/
                        }
                        if ($reportInfo->isSetAvailableDate()) 
                        {
							$report_info[$i]['available_date']	=	$reportInfo->getAvailableDate()->format(DATE_FORMAT);
                           /* echo("                    AvailableDate\n");
                            echo("                        " . $reportInfo->getAvailableDate()->format(DATE_FORMAT) . "\n");*/
                        }
                        if ($reportInfo->isSetAcknowledged()) 
                        {
							$report_info[$i]['acknowledge']	=	$reportInfo->getAcknowledged();
                            /*echo("                    Acknowledged\n");
                            echo("                        " . $reportInfo->getAcknowledged() . "\n");*/
                        }
                        if ($reportInfo->isSetAcknowledgedDate()) 
                        {
							$report_info[$i]['acknowledge_date']	=	$reportInfo->getAcknowledgedDate()->format(DATE_FORMAT);
                           /* echo("                    AcknowledgedDate\n");
                            echo("                        " . $reportInfo->getAcknowledgedDate()->format(DATE_FORMAT) . "\n");*/
                        }
						$i++;
                    }
					
                } 
               

                return $report_info;
     
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
	public function invokeRequestReport($report_type,$start_date = NULL,$end_date = NULL) {

		//echo $start_date.'-----'.$end_date;
		//die();
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
		 
		 $request->setMerchant($this->marchent_id);
		 $request->setReportType($report_type);
		 
		 if($start_date != NULL)	{
			 $tmp		=	new DateTime($start_date);
			 $request->setStartDate($tmp);
		 }
		 if($end_date != NULL)	{
			 $tmp		=	new DateTime($end_date);
			 $request->setEndDate($tmp);
		 }
		 
		 try {
			 
			 $response = $service->requestReport($request);
			 
			 $report_info	=	array();
			
			 
			 if ($response->isSetRequestReportResult()) { 
				  $requestReportResult = $response->getRequestReportResult();
				  if ($requestReportResult->isSetReportRequestInfo()) {
					  if ($requestReportResult->isSetReportRequestInfo()) {
                        
                        $reportRequestInfo = $requestReportResult->getReportRequestInfo();
                         
                          if ($reportRequestInfo->isSetReportRequestId()) 
                          {
							  $report_info['request_id']	=	$reportRequestInfo->getReportRequestId();	
                            /*  echo("                    ReportRequestId\n");
                              echo("                        " . $reportRequestInfo->getReportRequestId() . "\n");*/
                          }
                          if ($reportRequestInfo->isSetReportType()) 
                          {
							  $report_info['type']	=	$reportRequestInfo->getReportType();
                         /*     echo("                    ReportType\n");
                              echo("                        " . $reportRequestInfo->getReportType() . "\n");*/
							  
                          }
                          if ($reportRequestInfo->isSetStartDate()) 
                          {
							  $report_info['start_date_time']	=	$reportRequestInfo->getStartDate()->format(DATE_FORMAT);
                          /*    echo("                    StartDate\n");
                              echo("                        " . $reportRequestInfo->getStartDate()->format(DATE_FORMAT) . "\n");*/
                          }
                          if ($reportRequestInfo->isSetEndDate()) 
                          {
							  $report_info['end_date_time']	=	$reportRequestInfo->getEndDate()->format(DATE_FORMAT);
                         /*     echo("                    EndDate\n");
                              echo("                        " . $reportRequestInfo->getEndDate()->format(DATE_FORMAT) . "\n");*/
                          }
                          if ($reportRequestInfo->isSetSubmittedDate()) 
                          {
							  $report_info['submited_date_time']	=	$reportRequestInfo->getSubmittedDate()->format(DATE_FORMAT);
                            /*  echo("                    SubmittedDate\n");
                              echo("                        " . $reportRequestInfo->getSubmittedDate()->format(DATE_FORMAT) . "\n");*/
                          }
                          if ($reportRequestInfo->isSetReportProcessingStatus()) 
                          {
							  $report_info['status']	=	$reportRequestInfo->getReportProcessingStatus();
/*                              echo("                    ReportProcessingStatus\n");
                              echo("                        " . $reportRequestInfo->getReportProcessingStatus() . "\n");*/
                          }
                      }
					  return $report_info;
					  //return $reportRequestInfo->getReportRequestId();					
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
	
	public function saveTheOrdersData($report_id)	{
		//INTAILIZATION
		global $sql_obj;
		$acontents	=	$this->getReport($report_id);
		
		//TRUNCATE TABLE
		$sql_obj->Query("TRUNCATE TABLE sale_orders");

		/*echo "<pre>";
		print_r($acontents);
		die();*/
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
			$ship_id	=	$count		=	$sql_obj->RowCount("sale_orders","WHERE shipment_id = '".$array['3']."' AND order_id ='".$array['0']."'");
			//echo $ship_id;
			//die();
			if($ship_id == 0)	{
				
				//$count		=	$sql_obj->RowCount("sale_orders","WHERE order_id = '".$array['0']."'");
				//if($count > 0)	{
				//	echo "UPDATE ORDER ID: ".$array['0']."<br/>";
				//$sql_obj->Query("UPDATE sale_orders SET
					//			sales_price	 =  sales_price + '".$array['17']."',
					//			shipping_fee = shipping_fee + '".$array['19']."'
						//		WHERE order_id = '".$array['0']."'");
			//}else {
				echo "INSERTED ORDER ID: ".$array['0']."<br/>";
				$sql_obj->Query("INSERT INTO sale_orders ( 
									product_name, 
									transaction_date, 
									order_id, 
									sales_price, 
									shipping_fee, 
									sales_tax,
									sales_tax_service_fee, 
									fba_fee,
									shipment_id
								) VALUES ( 
									'".$array['14']."', 
									'$trans_date', 
									'".$array['0']."', 
									'".$array['17']."', 
									'".$array['19']."', 
									'".$array['20']."', 
									'".$array['20']."', 
									'$fba_fee',
									'".$array['3']."'
							)");
			//}
			}
			
		
		}
	}
	

	public function updateOrdersData($report_id)	{
		
		
		//INTAILIZATION
		global $sql_obj;
		$acontents					=	$this->getReport($report_id);
		
		//UPDATE TABLE BEFORE UPDATE LIVE
			$sql_obj->Query("UPDATE sale_orders SET 
									FBAPerOrderFulfillmentFee =  0,
									FBAPerUnitFulfillmentFee  =  0,
									FBAWeightBasedFee 		  =  0,
									Commission				  =  0,
									ShippingChargeback 		  =  0
						 ");
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
