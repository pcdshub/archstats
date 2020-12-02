#!/bin/bash

ARCH_HOST=${ARCH_HOST=localhost:17665}
APPLIANCES=${APPLIANCES=pscaa01 pscaa02}

echo "Caching information for testing from ${ARCH_HOST}..."
# curl ${ARCH_HOST}/mgmt/bpl/getApplianceMetrics  > archstats/tests/json/getApplianceMetrics.json
# curl ${ARCH_HOST}/mgmt/bpl/getStorageMetrics  > archstats/tests/json/getStorageMetrics.json
# curl ${ARCH_HOST}/mgmt/bpl/getInstanceMetrics  > archstats/tests/json/getInstanceMetrics.json
curl ${ARCH_HOST}/mgmt/bpl/getProcessMetrics  > archstats/tests/json/getProcessMetrics.json

 for appliance in ${APPLIANCES}; do
    echo "Appliance ${appliance}"
#     curl "${ARCH_HOST}/mgmt/bpl/getApplianceMetricsForAppliance?appliance=${appliance}"  > archstats/tests/json/getApplianceMetricsForAppliance-${appliance}.json
#     curl "${ARCH_HOST}/mgmt/bpl/getProcessMetricsDataForAppliance?appliance=${appliance}"  > archstats/tests/json/getProcessMetricsDataForAppliance-${appliance}.json
    # curl "${ARCH_HOST}/mgmt/bpl/getStorageMetricsForAppliance?appliance=${appliance}"  > archstats/tests/json/getStorageMetricsForAppliance-${appliance}.json
    # DO NOT include these -- too large:
    # curl "${ARCH_HOST}/mgmt/bpl/getCreationReportForAppliance?appliance=${appliance}"  > archstats/tests/json/getCreationReportForAppliance-${appliance}.json
 done
