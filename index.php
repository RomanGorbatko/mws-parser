<?php

include_once __DIR__ . '/MarketplaceWebService/Samples/reporting.php';
$config = include_once __DIR__ . '/config.php';

$pdo = new PDO('mysql:host=' . $config['pdo']['host'] . ';dbname=' . $config['pdo']['db'], $config['pdo']['user'], $config['pdo']['password']);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$acontents = getReport();

$query = '
    INSERT INTO sm_sales_monitoring
        (
            `status`, `snapshot_date`, `sku`, `fnsku`, `asin`, `product_name`, `conditions`, `sales_rank`, `product_group`, `total_quantity`, `sellable_quantity`, `unsellable_quantity`,
            `inv_age_0_to_90_days`, `inv_age_91_to_180_days`, `inv_age_181_to_270_days`, `inv_age_271_to_365_days`, `inv_age_365_plus_days`, `units_shipped_last_24_hrs`,
            `units_shipped_last_7_days`, `units_shipped_last_30_days`, `units_shipped_last_90_days`, `units_shipped_last_180_days`, `units_shipped_last_365_days`,
            `weeks_of_cover_t7`, `weeks_of_cover_t30`, `weeks_of_cover_t90`, `weeks_of_cover_t180`, `weeks_of_cover_t365`, `num_afn_new_sellers`, `num_afn_used_sellers`, `currency`,
            `your_price`, `sales_price`, `lowest_afn_new_price`, `lowest_afn_used_price`, `lowest_mfn_new_price`, `lowest_mfn_used_price`, `qty_to_be_charged_ltsf_12_mo`,
            `qty_in_long_term_storage_program`, `qty_with_removals_in_progress`, `projected_ltsf_12_mo`, `per_unit_volume`, `is_hazmat`, `in_bound_quantity`, `asin_limit`, 
            `inbound_recommend_quantity`, `qty_to_be_charged_ltsf_6_mo`, `projected_ltsf_6_mo`
        ) 
    VALUES 
        (
            :status, :snapshotDate, :sku, :fnsku, :asin, :productName, :conditions, :salesRank, :productGroup, :totalQuantity, :sellableQuantity, :unsellableQuantity,
            :invAge0To90Days, :invAge91To180Days, :invAge181To270Days, :invAge271To365Days, :invAge365PlusDays, :unitsShippedLast24Hrs, :unitsShippedLast7Days, :unitsShippedLast30Days,
            :unitsShippedLast90Days, :unitsShippedLast180Days, :unitsShippedLast365Days, :weeksOfCoverT7, :weeksOfCoverT30, :weeksOfCoverT90, :weeksOfCoverT180, :weeksOfCoverT365,
            :numAfnNewSellers, :numAfnUsedSellers, :currency, :yourPrice, :salesPrice, :lowestAfnNewPrice, :lowestAfnUsedPrice, :lowestMfnNewPrice, :lowestMfnUsedPrice, 
            :qtyToBeChargedLtsf12Mo, :qtyInLongTermStorageProgram, :qtyWithRemovalsInProgress, :projectedLtsf12Mo, :perUnitVolume, :isHazmat, :inBoundQuantity, :asinLimit,
            :inboundRecommendQuantity, :qtyToBeChargedLtsf6Mo, :projectedLtsf6Mo
        );'
;

foreach ($acontents as $key => $values) {
    $values = explode("\t", $values);

    if ($key == 0) {
        continue;
    }

    $map = generateMap($values);

    if ($id = checkProduct($values)) {
        $map['idSalesMonitoring'] = $id;

        updateValues($map);
    } else {
        insertValues($map);
    }
}

/**
 * @return array
 */
function getReport() {
    global $config;
    $amazone_array['AWS_API_KEY']			=	$config['mws']['API_KEY'];
    $amazone_array['AWS_API_SECRET_KEY']	=	$config['mws']['SECRET_KEY'];
    $amazone_array['MERCHANT_ID']			=	$config['mws']['MERCHANT_ID'];
    $amazone_array['MAKETPLACE_ID']			=	$config['mws']['MAKETPLACE_ID'];

    $report_obj = new Reporting($amazone_array);

    $report_type = '_GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA_';
    $report_req	= $report_obj->invokeGetReportList($report_type);
    unset($report_req['get_has_next']);

    $freshReportId = max(array_column($report_req, 'report_id'));

    return $report_obj->getReport($freshReportId);
}

function generateMap(array $values): array {
    return [
        'status' => "new",
        'snapshotDate' => date('Y-m-d', strtotime(trim($values[0]))),
        'sku' => $values[1] ?: '',
        'fnsku' => $values[2] ?: '',
        'asin' => $values[3] ?: '',
        'productName' => $values[4] ?: '',
        'conditions' => $values[5] ?: '',
        'salesRank' => $values[6] ?: 0,
        'productGroup' => $values[7] ?: '',
        'totalQuantity' => $values[8] ?: 0,
        'sellableQuantity' => $values[9] ?: 0,
        'unsellableQuantity' => $values[10] ?: 0,
        'invAge0To90Days' => $values[11] ?: 0,
        'invAge91To180Days' => $values[12] ?: 0,
        'invAge181To270Days' => $values[13] ?: 0,
        'invAge271To365Days' => $values[14] ?: 0,
        'invAge365PlusDays' => $values[15] ?: 0,
        'unitsShippedLast24Hrs' => $values[16] ?: 0,
        'unitsShippedLast7Days' => $values[17] ?: 0,
        'unitsShippedLast30Days' => $values[18] ?: 0,
        'unitsShippedLast90Days' => $values[19] ?: 0,
        'unitsShippedLast180Days' => $values[20] ?: 0,
        'unitsShippedLast365Days' => $values[21] ?: 0,
        'weeksOfCoverT7' => $values[22] ?: 0,
        'weeksOfCoverT30' => $values[23] ?: 0,
        'weeksOfCoverT90' => $values[24] ?: 0,
        'weeksOfCoverT180' => $values[25] ?: 0,
        'weeksOfCoverT365' => $values[26] ?: 0,
        'numAfnNewSellers' => $values[27] ?: 0,
        'numAfnUsedSellers' => $values[28] ?: 0,
        'currency' => $values[29] ?: '',
        'yourPrice' => $values[30] ?: 0,
        'salesPrice' => $values[31] ?: 0,
        'lowestAfnNewPrice' => $values[32] ?: 0,
        'lowestAfnUsedPrice' => $values[33] ?: 0,
        'lowestMfnNewPrice' => $values[34] ?: 0,
        'lowestMfnUsedPrice' => $values[35] ?: 0,
        'qtyToBeChargedLtsf12Mo' => $values[36] ?: 0,
        'qtyInLongTermStorageProgram' => $values[37] ?: 0,
        'qtyWithRemovalsInProgress' => $values[38] ?: 0,
        'projectedLtsf12Mo' => $values[39] ?: 0,
        'perUnitVolume' => $values[40] ?: 0,
        'isHazmat' => $values[41] ?: '',
        'inBoundQuantity' => $values[42] ?: 0,
        'asinLimit' => $values[43] ?: 0,
        'inboundRecommendQuantity' => $values[44] ?: 0,
        'qtyToBeChargedLtsf6Mo' => $values[45] ?: 0,
        'projectedLtsf6Mo' => $values[46] ?: 0,
    ];
}

/**
 * @param array $values
 * @return int
 */
function checkProduct(array $values): int {
    global $pdo;

    $stmt = $pdo->prepare('SELECT `id_sales_monitoring` FROM `sm_sales_monitoring` WHERE `sku` = "'. $values[1] .'" LIMIT 1');
    $stmt->execute();
    $row = $stmt->fetch();

    if (empty($row)) {
        return 0;
    }

    return $row['id_sales_monitoring'];
}

/**
 * @param array $map
 */
function insertValues(array $map) {
    global $query, $pdo;

    $pdo->prepare($query)->execute($map);
}

/**
 * @param array $map
 */
function updateValues(array $map) {
    global $query, $pdo;

    preg_match_all("/\`(.*?)\`/s", $query, $columns);
    preg_match_all("/\:(.*?)\,/s", $query, $params);
    $params[1][] = 'projectedLtsf6Mo';

    $updateQuery = 'UPDATE `sm_sales_monitoring` SET ';
    foreach ($columns[1] as $key => $column) {
        $updateQuery .= '`' . $column . '` = :' . $params[1][$key] . ', ';
    }

    $updateQuery = trim($updateQuery, ', ') . ' WHERE `id_sales_monitoring` = :idSalesMonitoring';

    $pdo->prepare($updateQuery)->execute($map);
}