scenario("weefinAssignCarbonIntensity resolves carbon intensity including null ESG framework") {

  Given("Input KPIs with corporate, sovereign, missing and null ESG framework carbon intensity")

  val inputDf = List(
    // --- corporate carbon intensity
    (daliId1, corporateIssuerTypeColumn, co2IntensityTco2eUsdMnRevenuesColName, "10.0"),

    // --- sovereign carbon intensity
    (daliId2, sovereignIssuerTypeColumn, carbonIntensityColName, "20.0"),

    // --- missing carbon intensity → NOT_RATED
    (daliId3, corporateIssuerTypeColumn, renewableEnergyColName, "1.0"),

    // --- null ESG framework → NOT_APPLICABLE
    (daliId4, null, carbonIntensityColName, "999.0")
  ).toDF(
    daliIdColName,
    esgFrameworkColName,
    kpiNameColName,
    kpiValueColName
  )

  When("weefinAssignCarbonIntensity is executed")

  val computedDf =
    new SecurityKpiDataFrameHelper(inputDf)
      .weefinAssignCarbonIntensity()
      .select(daliIdColName, kpiNameColName, kpiValueColName)

  Then("Carbon intensity KPI is correctly assigned for all cases")

  val expectedDf = List(
    // preserved values
    (daliId1, kpiCarbonIntensityColName, "10.0"),
    (daliId2, kpiCarbonIntensityColName, "20.0"),

    // filled by fill_carbon_intensity_kpi
    (daliId3, kpiCarbonIntensityColName, notRated),

    // null ESG framework → NOT_APPLICABLE
    (daliId4, kpiCarbonIntensityColName, notApplicable),

    // non-carbon KPI preserved
    (daliId3, renewableEnergyColName, "1.0")
  ).toDF(
    daliIdColName,
    kpiNameColName,
    kpiValueColName
  )

  assertDataFrameEquals(
    expectedDf.orderBy(daliIdColName, kpiNameColName),
    computedDf.orderBy(daliIdColName, kpiNameColName)
  )
}
