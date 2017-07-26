<?php
/** 
 *  PHP Version 5
 *
 *  @category    Amazon
 *  @package     MarketplaceWebService
 *  @copyright   Copyright 2009 Amazon Technologies, Inc.
 *  @link        http://aws.amazon.com
 *  @license     http://aws.amazon.com/apache2.0  Apache License, Version 2.0
 *  @version     2009-01-01
 */
/******************************************************************************* 

 *  Marketplace Web Service PHP5 Library
 *  Generated: Thu May 07 13:07:36 PDT 2009
 * 
 */

/**
 * Get Report  Sample
 */
require_once('../../../../includes.php');

include_once ('.config.inc.php'); 

/************************************************************************
* Uncomment to configure the client instance. Configuration settings
* are:
*
* - MWS endpoint URL
* - Proxy host and port.
* - MaxErrorRetry.
***********************************************************************/
// IMPORTANT: Uncomment the approiate line for the country you wish to
// sell in:
// United States:
$serviceUrl = "https://mws.amazonservices.com";
// United Kingdom
//$serviceUrl = "https://mws.amazonservices.co.uk";
// Germany
//$serviceUrl = "https://mws.amazonservices.de";
// France
//$serviceUrl = "https://mws.amazonservices.fr";
// Italy
//$serviceUrl = "https://mws.amazonservices.it";
// Japan
//$serviceUrl = "https://mws.amazonservices.jp";
// China
//$serviceUrl = "https://mws.amazonservices.com.cn";
// Canada
//$serviceUrl = "https://mws.amazonservices.ca";
// India
//$serviceUrl = "https://mws.amazonservices.in";

$config = array (
  'ServiceURL' => $serviceUrl,
  'ProxyHost' => null,
  'ProxyPort' => -1,
  'MaxErrorRetry' => 3,
);

/************************************************************************
 * Instantiate Implementation of MarketplaceWebService
 * 
 * AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY constants 
 * are defined in the .config.inc.php located in the same 
 * directory as this sample
 ***********************************************************************/
 $service = new MarketplaceWebService_Client(
     AWS_ACCESS_KEY_ID, 
     AWS_SECRET_ACCESS_KEY, 
     $config,
     APPLICATION_NAME,
     APPLICATION_VERSION);
 
/************************************************************************
 * Uncomment to try out Mock Service that simulates MarketplaceWebService
 * responses without calling MarketplaceWebService service.
 *
 * Responses are loaded from local XML files. You can tweak XML files to
 * experiment with various outputs during development
 *
 * XML files available under MarketplaceWebService/Mock tree
 *
 ***********************************************************************/
 // $service = new MarketplaceWebService_Mock();

/************************************************************************
 * Setup request parameters and uncomment invoke to try out 
 * sample for Get Report Action
 ***********************************************************************/
 // @TODO: set request. Action can be passed as MarketplaceWebService_Model_GetReportRequest
 // object or array of parameters
 $reportId = '20751275363';
 
 $parameters = array (
   'Merchant' => MERCHANT_ID,
   'Report' => @fopen('php://memory', 'rw+'),
   'ReportId' => $reportId,
   'MWSAuthToken' => '<MWS Auth Token>', // Optional
 );
 $request = new MarketplaceWebService_Model_GetReportRequest($parameters);

$request = new MarketplaceWebService_Model_GetReportRequest();
$request->setMerchant(MERCHANT_ID);
$request->setReport(@fopen('php://memory', 'rw+'));
$request->setReportId($reportId);
//$request->setMWSAuthToken('<MWS Auth Token>'); // Optional
 
invokeGetReport($service, $request);

/**
  * Get Report Action Sample
  * The GetReport operation returns the contents of a report. Reports can potentially be
  * very large (>100MB) which is why we only return one report at a time, and in a
  * streaming fashion.
  *   
  * @param MarketplaceWebService_Interface $service instance of MarketplaceWebService_Interface
  * @param mixed $request MarketplaceWebService_Model_GetReport or array of parameters
  */
  function invokeGetReport(MarketplaceWebService_Interface $service, $request) 
  {
      try {
              $response = $service->getReport($request);
			  echo "<pre>";
             // print_r($response);
			  //die();
               /* echo ("Service Response\n");
                echo ("=============================================================================\n");

                echo("        GetReportResponse\n");
                if ($response->isSetGetReportResult()) {
                  $getReportResult = $response->getGetReportResult(); 
                  echo ("            GetReport");
                  
                  if ($getReportResult->isSetContentMd5()) {
                    echo ("                ContentMd5");
                    echo ("                " . $getReportResult->getContentMd5() . "\n");
                  }
                }
                if ($response->isSetResponseMetadata()) { 
                    echo("            ResponseMetadata\n");
                    $responseMetadata = $response->getResponseMetadata();
                    if ($responseMetadata->isSetRequestId()) 
                    {
                        echo("                RequestId\n");
                        echo("                    " . $responseMetadata->getRequestId() . "\n");
                    }
                }*/
                
                //echo ("        Report Contents\n");
                //echo (stream_get_contents($request->getReport()) . "\n");
				$contents = stream_get_contents($request->getReport());
				//echo $contents;
				
				//$brcontent = str_replace("\n",'-test-',$contents);
				//echo $brcontent;
				$acontents = explode("\n", $contents);
				//print_r($acontents);
				
				 
				for($i=0; $i<count($acontents); $i++){
					$st= explode("\t", $acontents[$i]);
					//print_r ($str);
					if($i>0){	
					//for( $j=0; $i<count($st); $j++){
					//$sql_obj->Query("insert into inventory_health(product_details)values('".$acontents[$i]."')") or die(mysql_error());
					mysql_query("insert into inventary_health(sku, fnsku, asin, product_name, condition_, your_price, mfn_listing_exists, mfn_fulfillable_quantity, afn_listing_exists, afn_warehouse_quantity, afn_fulfillable_quantity, afn_unsellable_quantity, afn_reserved_quantity, afn_total_quantity, per_unit_volume, afn_inbound_working_quantity, afn_inbound_shipped_quantity, afn_inbound_receiving_quantity)values('".$st[0]."', '".$st[1]."', '".$st[2]."', '".$st[3]."', '".$st[4]."', '".$st[5]."', '".$st[6]."', '".$st[7]."', '".$st[8]."', '".$st[9]."', '".$st[10]."', '".$st[11]."', '".$st[12]."', '".$st[13]."', '".$st[14]."', '".$st[15]."', '".$st[16]."', '".$st[17]."')") or die(mysql_error());
					//}
					}
				}
				
				foreach($acontents as $a){
					/*$strc = implode(" ", $a);
					echo $strc;*/
					
				}
				//echo nl2br($contents);
				die();
				//echo str_replace("\t",'-test-',$contents);
				
				/*while(){
				}*/
				//loo for text file for each line
					//make array for this line
					//run query save to db
				//end loop
				/*$file = $request->getReport();
				$handle = fopen($file,r);
				$flarray = array();
				while (($line = fgets($handle)) !== false) {
					$flarray[] = $line;
                 }
				 fclose($handle);
				 print_r($flarray);*/
				 //fread('', );
				/*$farray = explode("\t",$contents);
				echo "<pre>";
				print_r($farray);
				echo "</pre>";*/
				//die();
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
                                                                                
