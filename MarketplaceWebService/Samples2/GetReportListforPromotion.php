<?php


require_once('../../../../includes.php');
include_once ('.config.inc.php'); 


$serviceUrl = "https://mws.amazonservices.com";



$config = array (
  'ServiceURL' => $serviceUrl,
  'ProxyHost' => null,
  'ProxyPort' => -1,
  'MaxErrorRetry' => 3,
);

 $service = new MarketplaceWebService_Client(
     AWS_ACCESS_KEY_ID, 
     AWS_SECRET_ACCESS_KEY, 
     $config,
     APPLICATION_NAME,
     APPLICATION_VERSION);

 
 $request = new MarketplaceWebService_Model_GetReportListRequest();
 $request->setMerchant(MERCHANT_ID);
 /////////////////////////
 $report_type_array = new MarketplaceWebService_Model_TypeList();
 $report_type_array->setType('_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA_');
 $request->setReportTypeList($report_type_array);
 ////////////////////////
 $request->setAvailableToDate(new DateTime('now', new DateTimeZone('UTC')));
 $request->setAvailableFromDate(new DateTime('-3 months', new DateTimeZone('UTC')));
 $request->setAcknowledged(false);
// $request->setMWSAuthToken('<MWS Auth Token>'); // Optional
 
 invokeGetReportList($service, $request);
                                                                    

  function invokeGetReportList(MarketplaceWebService_Interface $service, $request) 
  {
      try {
              $response = $service->getReportList($request);
              
               
                if ($response->isSetGetReportListResult()) { 
                    
                    $getReportListResult = $response->getGetReportListResult();
                    
                   
                    $reportInfoList = $getReportListResult->getReportInfoList();
					mysql_query("TRUNCATE TABLE reports");
                    foreach ($reportInfoList as $reportInfo) {
						
                       $report_id = $reportInfo->getReportId();
					//echo $report_id."<br />";
					  
                   mysql_query("INSERT INTO `promotion` ( `report_type`, `report_date`, `report_id`, `date_time`) VALUES ( ' " . $reportInfo->getReportType() . "', '" . $reportInfo->getAvailableDate()->format(DATE_FORMAT) . "', '$report_id', CURRENT_TIMESTAMP)");					   
				   
				   /////////
                            /*echo("                        " . $reportInfo->getReportId() . "\n");
                        
                        
                            
                            echo("                        " . $reportInfo->getReportType() . "\n");
                        
                        
                        
                            echo("                        " . $reportInfo->getAvailableDate()->format(DATE_FORMAT) . "\n");*/
                        
                        
                        
                    }
					die('ok'); 
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
?>
