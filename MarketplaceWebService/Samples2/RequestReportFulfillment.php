<?php

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
 

$marketplaceIdArray = array("Id" => array('ATVPDKIKX0DER'));


 
 $parameters = array (
   'Merchant' => MERCHANT_ID,
   'MarketplaceIdList' => $marketplaceIdArray,
   'ReportType' => '_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_',
   'ReportOptions' => 'ShowSalesChannel=true',
   //'MWSAuthToken' => '<MWS Auth Token>', // Optional
 );
 
 $request = new MarketplaceWebService_Model_RequestReportRequest($parameters);
 
 $request = new MarketplaceWebService_Model_RequestReportRequest();
 $request->setMarketplaceIdList($marketplaceIdArray);
 $request->setMerchant(MERCHANT_ID);
 $request->setReportType('_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_');
 //$request->setMWSAuthToken('<MWS Auth Token>'); // Optional


 invokeRequestReport($service, $request);
 

  function invokeRequestReport(MarketplaceWebService_Interface $service, $request) 
  {
      try {
              $response = $service->requestReport($request);
              
                echo ("Service Response\n");
                echo ("=============================================================================\n");

               
                if ($response->isSetRequestReportResult()) { 
                   
                    $requestReportResult = $response->getRequestReportResult();
                    
                    if ($requestReportResult->isSetReportRequestInfo()) {
                        
                        $reportRequestInfo = $requestReportResult->getReportRequestInfo();
                         
                          if ($reportRequestInfo->isSetReportRequestId()) 
                          {
                              echo("                    ReportRequestId\n");
                              echo("                        " . $reportRequestInfo->getReportRequestId() . "\n");
                          }
                          if ($reportRequestInfo->isSetReportType()) 
                          {
                              echo("                    ReportType\n");
                              echo("                        " . $reportRequestInfo->getReportType() . "\n");
                          }
                          if ($reportRequestInfo->isSetStartDate()) 
                          {
                              echo("                    StartDate\n");
                              echo("                        " . $reportRequestInfo->getStartDate()->format(DATE_FORMAT) . "\n");
                          }
                          if ($reportRequestInfo->isSetEndDate()) 
                          {
                              echo("                    EndDate\n");
                              echo("                        " . $reportRequestInfo->getEndDate()->format(DATE_FORMAT) . "\n");
                          }
                          if ($reportRequestInfo->isSetSubmittedDate()) 
                          {
                              echo("                    SubmittedDate\n");
                              echo("                        " . $reportRequestInfo->getSubmittedDate()->format(DATE_FORMAT) . "\n");
                          }
                          if ($reportRequestInfo->isSetReportProcessingStatus()) 
                          {
                              echo("                    ReportProcessingStatus\n");
                              echo("                        " . $reportRequestInfo->getReportProcessingStatus() . "\n");
                          }
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
 
?>

                                                                                
