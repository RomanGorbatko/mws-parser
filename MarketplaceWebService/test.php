<?php
$AWSAccessKeyId = "23240--CPAI74RPBXQ3ITQ";
$SecretAccessKey = "+YT9iFwK339TWHssssssssssss333333a";
 
$ItemId = "B00JUEVZI8"; // ASIN
$Timestamp = gmdate("Y-m-d\TH:i:s\Z");
$Timestamp = str_replace(":", "%3A", $Timestamp);
$ResponseGroup = "ItemAttributes,Offers,Images,Reviews";
$ResponseGroup = str_replace(",", "%2C", $ResponseGroup);
 
$String = "AWSAccessKeyId=$AWSAccessKeyId&
ItemId=$ItemId&
Operation=ItemLookup&
AssociateTag=test05-12&
ResponseGroup=$ResponseGroup&
Service=AWSECommerceService&
Timestamp=$Timestamp&
Version=2011-10-01";
 
$String = str_replace("\n", "", $String);
 
$Prepend = "GET\nwebservices.amazon.com\n/onca/xml\n";
$PrependString = $Prepend . $String;
 
$Signature = base64_encode(hash_hmac("sha256", $PrependString, $SecretAccessKey, True));  
$Signature = str_replace("+", "%2B", $Signature);
$Signature = str_replace("=", "%3D", $Signature);
 
$BaseUrl = "http://webservices.amazon.com/onca/xml?";
$SignedRequest = $BaseUrl . $String . "&Signature=" . $Signature;
 echo $SignedRequest;
//$XML = simplexml_load_file($SignedRequest);
 //echo "<pre>";
//echo '<a href="'.$SignedRequest.'">XML</a><p>';
//print_r ($XML);
