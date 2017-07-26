<?php
set_include_path('/home/dixeam/public_html/amazone/includes/lib/');
require_once('includes.php'); 
require_once('reporting.php');


$amazone_array['AWS_API_KEY']			=	'AKIAJXZHJ4W775V6BPLQ';
$amazone_array['AWS_API_SECRET_KEY']	=	'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';
$amazone_array['MERCHANT_ID']			=	'A196A8GC0YNHR';
$amazone_array['MAKETPLACE_ID']			=	'ATVPDKIKX0DER';


$report_obj		=		new Reporting($amazone_array);
$report_type	=		'_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_';

//CHACKING ORDER DATE IN USER TABLE

$user_open_date		=			$sql_obj->QFetchArray("SELECT order_date FROM users where order_flag = '0' LIMIT 1");

if(mysql_num_rows($user_open_date) != '1'){
	
	

//GET THE USER FIRST PRODUCT DATE
$pro_open_date		=			$sql_obj->QFetchArray("SELECT open_date FROM products ORDER BY open_date ASC LIMIT 1");

$start_date			=			$pro_open_date['open_date'];



$next_date			=			date('Y-m-d 00:00:00', strtotime($start_date. ' + 30 days'));


}else{
	
	

$start_date			=			$user_open_date['order_date'];	

$next_date			=			date('Y-m-d 00:00:00', strtotime($start_date. ' + 30 days'));
	
	}
	
	
	$report_req		=	$report_obj->invokeRequestReport($report_type,$start_date,$next_date);
	
	print_r($report_req);
	die();

/*$today_date			=   		date('Y-m-d 00:00:00');
$start_date			=			date('Y-m-d 00:00:00',strtotime($pro_open_date['open_date']));
$i					=			0;
$sql_obj->Query("DELETE FROM reports WHERE report_type = '".$report_type."'");
while(strtotime($start_date) < strtotime($today_date))	{
	$next_date		=	date('Y-m-d 00:00:00', strtotime($start_date. ' + 30 days'));
	
	echo $i."=".$start_date.'-----'.$next_date."<br/>";
	
	

	//REPORT TO GET THE ALL ORDERS
	$report_req		=	$report_obj->invokeRequestReport($report_type,$start_date,$next_date);
	
	$sql_obj->Query("INSERT INTO reports (report_type, report_req_id, submitted_date_time, start_date_time, end_date_time, report_id, date_time, user_id, status) VALUES ('".$report_type."', '".$report_req['request_id']."', '".$report_req['submited_date_time']."', '".$report_req['start_date_time']."', '".$report_req['end_date_time']."', '', CURRENT_TIMESTAMP, '12', '".$report_req['status']."')");
	$start_date	=	$next_date;
	$i++;
	
	echo "<pre>";
	print_r($report_req);
	
}
*/
?>
