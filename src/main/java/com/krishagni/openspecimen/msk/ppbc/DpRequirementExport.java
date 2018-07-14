package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import com.krishagni.catissueplus.core.administrative.domain.DistributionProtocol;
import com.krishagni.catissueplus.core.administrative.events.DpRequirementDetail;
import com.krishagni.catissueplus.core.administrative.services.DistributionProtocolService;
import com.krishagni.catissueplus.core.common.events.RequestEvent;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;

@Configurable
public class DpRequirementExport {
	private CsvFileWriter csvFileWriter;
	
	@Autowired
	private DistributionProtocolService dpSvc;
	
	public DpRequirementExport() {
		this.csvFileWriter = getCSVWriter();
		this.csvFileWriter.writeNext(getHeader());
	}
	
	public void exportDPRequirement(DistributionProtocol dp) {
		List<DpRequirementDetail> dpRequirements = dpSvc.getRequirements(getRequest(dp.getId())).getPayload();
		if (!dpRequirements.isEmpty()) {
			dpRequirements.forEach(dpr -> exportDpR(dpr, dp));
		}
	}
	
	public void closeWriter() {
		IOUtils.closeQuietly(csvFileWriter);
	}
	
	public void flushCsvWriter() throws IOException {
		csvFileWriter.flush();
	}

	private void exportDpR(DpRequirementDetail dpr, DistributionProtocol dp) {
		csvFileWriter.writeNext(getRow(dpr, dp));
	}

	private String[] getRow(DpRequirementDetail dpr, DistributionProtocol dp) {
		return new String[] {
			dpr.getSpecimenType(),
			dpr.getAnatomicSite(),
			dpr.getPathologyStatuses().iterator().next(),
			dpr.getQuantity().toString(),
			dpr.getCost().toString(),
			dp.getId().toString()
		};
	}

	private CsvFileWriter getCSVWriter() {
		String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		File outputFile = new File(ConfigUtil.getInstance().getDataDir(), "DpRequirement_" + timestamp + ".csv");
		return CsvFileWriter.createCsvFileWriter(outputFile);
	}

	private String[] getHeader() {
		return new String[] {
				"TBRD_SPECIMEN_TYPE_CD",
				"TBRD_SITE_DESC",
				"TBRD_CATEGORY_DESC",
				"TBRD_EXPECTED_AMT", 
				"TBRD_BILLING_AMT",
				"TBRD_SOURCE_REQUEST"
		};
	}
	
	private <T> RequestEvent<T> getRequest(T payload) {
		return new RequestEvent<T>(payload);
	}
}