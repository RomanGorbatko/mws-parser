<?php
set_include_path('/home/dixeam/public_html/amazone/includes/lib/');
require_once('includes.php'); 
require_once('reporting.php');


$amazone_array['AWS_API_KEY']			=	'AKIAJXZHJ4W775V6BPLQ';
$amazone_array['AWS_API_SECRET_KEY']	=	'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';
$amazone_array['MERCHANT_ID']			=	'A196A8GC0YNHR';
$amazone_array['MAKETPLACE_ID']			=	'ATVPDKIKX0DER';

$user_row		=	$sql_obj->QFetchArray("SELECT * FROM users WHERE inventory = 0 ORDER BY id DESC LIMIT 1");
if(is_array($user_row))	{
	$report_obj		=	new Reporting($amazone_array);
	$report_type	=	array();
	$report_type	=	'_GET_MERCHANT_LISTINGS_DATA_';
	
	//GET REPORT IS REPORT ID EXIST
	$report_row		=	$sql_obj->QFetchArray("SELECT * FROM reports WHERE report_type = '$report_type'");
	if(is_array($report_row))	{
		if($report_row['report_id'] == '')	{
			$report_req		=		$report_obj->invokeGetReportList($report_type);
			for($i = 0; $i < count($report_req); $i++)	{
				if(isset($report_req[$i]['req_id']))	{
					$sql_obj->Query("UPDATE reports SET report_id = '".$report_req[$i]['report_id']."' WHERE report_type = '$report_type' AND report_req_id = '".$report_req[$i]['req_id']."'");
					echo "REPORT ID UPDATED <br>";
				}
			}
						
		}else {
			//UPDATE THE DATABASE
			$report_obj->UpdateInventory($report_row['report_id'],$user_row['id']); 
			
			//DELETE REPORT FOR NEW GENRATE
			$sql_obj->Query("DELETE FROM reports WHERE report_type = '$report_type'");
			
			//UPDATE USER FOR NEXT USER
			$sql_obj->Query("UPDATE users SET inventory = 1 WHERE id = '".$user_row['id']."'");
			echo "DATA UPDATED<br/>";
		}
	}else {
		$report_req		=	$report_obj->invokeRequestReport($report_type);
		$sql_obj->Query("INSERT INTO reports (report_type, report_req_id, submitted_date_time, start_date_time, end_date_time, report_id, date_time, user_id, status) VALUES ('".$report_type."', '".$report_req['request_id']."', '".$report_req['submited_date_time']."', '".$report_req['start_date_time']."', '".$report_req['end_date_time']."', '', CURRENT_TIMESTAMP, '12', '".$report_req['status']."')");
		echo "Report Invoke<br/>";
	}
}else {
	$sql_obj->Query("UPDATE users SET inventory = 0");
	echo "ALL USERS UPDATED TO \'0\'<br/>";
}
exit();
?>
