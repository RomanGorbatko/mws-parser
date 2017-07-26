<?php
set_include_path('/home/dixeam/public_html/amazone/includes/lib/');
require_once('includes.php'); 
require_once('reporting.php');


$amazone_array['AWS_API_KEY']			=	'AKIAJXZHJ4W775V6BPLQ';
$amazone_array['AWS_API_SECRET_KEY']	=	'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';
$amazone_array['MERCHANT_ID']			=	'A196A8GC0YNHR';
$amazone_array['MAKETPLACE_ID']			=	'ATVPDKIKX0DER';

$report_row		=	$sql_obj->QFetchArray("SELECT * FROM reports WHERE report_type = '_GET_FLAT_FILE_PAYMENT_SETTLEMENT_DATA_' ORDER BY date_time DESC LIMIT 1");

$report_obj		=		new Reporting($amazone_array);
$report_obj->updateOrdersData($report_row['report_id']);


?>
