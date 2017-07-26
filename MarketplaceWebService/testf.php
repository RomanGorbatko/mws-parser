<?php
$AWSAccessKeyId = "AKIAJXZHJ4-----------------";
$SecretAccessKey = "y+23fIarxTEzt7MCpHhebL1Uw3Ig3MgMFW/Bj5+e";
 
$ItemId = "B00G8S4LXK"; // ASIN
$Timestamp = gmdate("Y-m-d\TH:i:s\Z");
$Timestamp = str_replace(":", "%3A", $Timestamp);
$ResponseGroup = "ItemAttributes,Offers,Images,Reviews";
$ResponseGroup = str_replace(",", "%2C", $ResponseGroup);
 
$String = "AWSAccessKeyId=$AWSAccessKeyId&Action=GetMyPriceForASINp&SellerId=A196A000----&SignatureVersion=2&Timestamp=$Timestamp&Version=2011-10-01";
 
 //ItemId=$ItemId&

$String = str_replace("\n", "", $String);
 
//$Prepend = "/Products/2011-10-01?";
//$PrependString = $Prepend . $String;
 
$Signature = base64_encode(hash_hmac("sha256", $String, $SecretAccessKey, True));  
$Signature = str_replace("+", "%2B", $Signature);
$Signature = str_replace("=", "%3D", $Signature);
 
$BaseUrl = "https://mws.amazonservices.com/Products/2011-10-01?";
$SignedRequest = $BaseUrl . $String . "&Signature=" . $Signature."&SignatureMethod=HmacSHA256&MarketplaceId=ATVPDKIK-------
  &ASINList.ASIN.1=B00G8S4LXK";
 echo $SignedRequest;
$XML = simplexml_load_file($SignedRequest);
 echo "<pre>";
echo '<a href="'.$SignedRequest.'">XML</a><p>';
print_r ($XML);
