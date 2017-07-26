<?php
//INCLUDE THE FILE WITH THE ABOSULE PATH
$dir_include	=	"thirdparty/amazone";
set_include_path('/home/dixeam/public_html/amazone/includes/lib/');
require_once('includes.php'); 
require_once($dir_include.'/MarketplaceWebService/Samples/reporting.php');
//END OF INCLUDE THE FILE WITH THE ABOSULE PATH





function getDates()	{ 

	global $sql_obj;
	//CHACKING ORDER DATE IN USER TABLE
	$open_date			=			"";
	$user_open_date		=			$sql_obj->QFetchArray("SELECT order_date FROM users where orders = '0' LIMIT 1");
	if($user_open_date['order_date'] == '0000-00-00 00:00:00')	{
		$user_row	=	$sql_obj->QFetchArray("SELECT open_date FROM products ORDER BY open_date ASC LIMIT 1");
		$open_date	=	$user_row['open_date'];
	}else	{
		$open_date	=	$user_open_date['order_date'];
	}
	$dates['pre_date']		=	date('Y-m-d 00:00:00', strtotime($open_date));
	//GETTING THE NEXT DATE TO CREATE REPORT FOR 30 DAYS
	$dates['next_date']		=	date('Y-m-d 00:00:00', strtotime($open_date. ' + 30 days'));
	return $dates;
}
function updateUser($report_type)	{
	global $sql_obj;
	$dates		=	getDates();
	$sql_obj->Query("UPDATE users SET order_date = '".$dates['next_date']."'");
	$sql_obj->Query("DELETE FROM reports WHERE report_type = '$report_type'");
	
}
function invokeReport($report_type, $amazone_array, $report_obj)	{
	global $sql_obj;
	$dates	=	getDates();
	//INVOKING THE REPORT
	$report_req		=	$report_obj->invokeRequestReport($report_type,$dates['pre_date'],$dates['next_date']);
	$sql_obj->Query("INSERT INTO reports (report_type, report_req_id, submitted_date_time, start_date_time, end_date_time, report_id, date_time, user_id, status) VALUES ('".$report_type."', '".$report_req['request_id']."', '".$report_req['submited_date_time']."', '".$report_req['start_date_time']."', '".$report_req['end_date_time']."', '', CURRENT_TIMESTAMP, '12', '".$report_req['status']."')");
	echo "REPORT INVOKE<br/>";
	print_r($report_req);
}
function getUserCredentials($uid)	{
	global $sql_obj;
	$usetting_row	=	$sql_obj->QFetchArray("SELECT * FROM setting WHERE user_id ='$uid'");
	$amazone_array['AWS_API_KEY']				=	'AKIAJXZHJ4W775V6BPLQ';
	$amazone_array['AWS_API_SECRET_KEY']		=	'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';
	$amazone_array['MERCHANT_ID']				=	$usetting_row['marketplace_seller_id'];
	$amazone_array['MAKETPLACE_ID']				=	$usetting_row['marketplace_id'];
	
	
	/*$amazone_array['AWS_API_KEY']			=	'AKIAJXZHJ4W775V6BPLQ';
	$amazone_array['AWS_API_SECRET_KEY']	=	'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';
	$amazone_array['MERCHANT_ID']			=	'A196A8GC0YNHR';
	$amazone_array['MAKETPLACE_ID']			=	'ATVPDKIKX0DER';*/
	
	return $amazone_array;
}







//GET USER FOR UPDATE AND HIS/HER SETTING
$user_row		=		$sql_obj->QFetchArray("SELECT * FROM users WHERE orders = '0' LIMIT 1");
$amazone_array	=	getUserCredentials($user_row['id']);




//GET THE REPORTING
$report_obj		=		new Reporting($amazone_array);
$report_type	=		'_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_';
$report_row		=		$sql_obj->QFetchArray("SELECT * FROM reports WHERE report_type = '$report_type'");



if(is_array($report_row))	{
	if($report_row['report_id'] == "")	{
		$reports		=		$report_obj->invokeGetReportReqList($report_type);
		for($i = 0; $i < count($reports); $i++)	{
			if(trim($reports[$i]['req_id']) == trim($report_row['report_req_id']))	{
				if($reports[$i]['status'] == '_DONE_')	{
					
					//UPDATE THE DATABASE
					$report_obj->saveTheOrdersData($reports[$i]['report_id'],$user_row['id']); 
					echo "DATA UPDATED <br>";
					
					//DELTE THE DATABASE
					$sql_obj->Query("DELETE FROM reports WHERE report_type = '$report_type'");
					echo "<br/>-------------------------<br/>".
						 "REPORT DELETED <br>";
				}else {
					updateUser($report_type);
					echo "REPORT NOT DONE | DELETE REPORT | OREDER DATE UPDATE<br/>";
				}
			}
		}
	}
}else	{
	invokeReport($report_type, $amazone_array, $report_obj);
}


?>
