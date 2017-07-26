<?php
set_include_path('/home/dixeam/public_html/amazone/includes/lib/');
require_once('includes.php'); 
require_once('reporting.php');


$amazone_array['AWS_API_KEY']			=	'AKIAJXZHJ4W775V6BPLQ';
$amazone_array['AWS_API_SECRET_KEY']	=	'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';
$amazone_array['MERCHANT_ID']			=	'A196A8GC0YNHR';
$amazone_array['MAKETPLACE_ID']			=	'ATVPDKIKX0DER';


$report_obj		=		new Reporting($amazone_array);

$report_type	=	array();

//......................................FOR SALE DASHBOARD.................//
//REPORT TO GET THE ALL ORDERS
$report_type[0]	=	'_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_';
//REPORT TO GET THE EXPENSECE AGAIN ABOVER ORDERS
$report_type[1]	=	'_GET_FLAT_FILE_PAYMENT_SETTLEMENT_DATA_';
//......................................END OF SALE DASHBOARD.................//

//....................FOR UPDATE INVENTORY................................//
$report_type[2]	=	'_GET_MERCHANT_LISTINGS_DATA_';
//....................END OF UPDATE INVENTORY................................//


//....................FOR UPDATE INVENTORY HEALTH................................//
$report_type[3]	=	'_GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA_';
//....................END OF UPDATE INVENTORY HEALTH................................//

for($i = 0; $i < count($report_type); $i++ )	{
	$report_req		=	$report_obj->invokeRequestReport($report_type[$i]);
	echo "Report_Requested: ".$report_type[$i]."===="."Requested ID: ".$report_req."<br/>";
}
?>
