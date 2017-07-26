<?php
set_include_path('/home/dixeam/public_html/amazone/includes/lib/');
require_once('includes.php'); 
require_once('reporting.php');


$amazone_array['AWS_API_KEY']			=	'AKIAJXZHJ4W775V6BPLQ';
$amazone_array['AWS_API_SECRET_KEY']	=	'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';
$amazone_array['MERCHANT_ID']			=	'A196A8GC0YNHR';
$amazone_array['MAKETPLACE_ID']			=	'ATVPDKIKX0DER';


$report_type	=		'_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_';
$report_obj		=		new Reporting($amazone_array);
$reports		=		$report_obj->invokeGetReportList($report_type); 

print_r($reports);
die(); 
for($i = 0; $i < count($reports); $i++)	{
	if($reports[$i]['report_id'] != "")	{
		$sql_obj->Query("UPDATE reports SET report_id = '".$reports[$i]['report_id']."' WHERE report_req_id = '".trim($reports[$i]['req_id'])."'");
	}
}
echo "<pre>";
print_r($reports);
die();
?>
