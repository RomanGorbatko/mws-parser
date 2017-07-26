<?php
$AWSAccessKeyId = "2342JXZHJ4-----------------";
$SecretAccessKey = "-7867-3fIarxTEzt7MCpH6433333336---------";
 
$ItemId = "B00G8S4LXK"; // ASIN
$Timestamp = gmdate("Y-m-d\TH:i:s\Z");
$Timestamp = str_replace(":", "%3A", $Timestamp);
$ResponseGroup = "ItemAttributes,Offers,Images,Reviews";
$ResponseGroup = str_replace(",", "%2C", $ResponseGroup);
 
$String = "AWSAccessKeyId=$AWSAccessKeyId&

Action=GetMyPriceForASINp&
SellerId=323678-6A000----&
SignatureVersion=2&

Timestamp=$Timestamp&
Version=2011-10-01";
 
 //ItemId=$ItemId&

$String = str_replace("\n", "", $String);
 
$Prepend = "/Products/2011-10-01";
$PrependString = $Prepend . $String;
 
$Signature = base64_encode(hash_hmac("sha256", $PrependString, $SecretAccessKey, True));  
$Signature = str_replace("+", "%2B", $Signature);
$Signature = str_replace("=", "%3D", $Signature);
 
$BaseUrl = "http://mws.amazonservices.com/Products/2011-10-01?";
$SignedRequest = $BaseUrl . $String . "&Signature=" . $Signature."&SignatureMethod=HmacSHA256&MarketplaceId=233534e5-VPDKIK-------
  &ASINList.ASIN.1=B00G8S4LXK HTTP/1.1";
 echo $SignedRequest;
$XML = simplexml_load_file($SignedRequest);
 echo "<pre>";
echo '<a href="'.$SignedRequest.'">XML</a><p>';
print_r ($XML);
