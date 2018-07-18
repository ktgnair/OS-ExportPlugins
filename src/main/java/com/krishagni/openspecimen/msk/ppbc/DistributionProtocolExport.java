package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import com.krishagni.catissueplus.core.administrative.domain.DistributionProtocol;
import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.repository.DpListCriteria;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.repository.DaoFactory;
import com.krishagni.catissueplus.core.common.PlusTransactional;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;
import com.krishagni.catissueplus.core.de.domain.DeObject.Attr;

@Configurable
public class DistributionProtocolExport implements ScheduledTask {
	private static final Log logger = LogFactory.getLog(DistributionProtocolExport.class);

	@Autowired
	private DaoFactory daoFactory;
	
	public void doJob(ScheduledJobRun jobRun) {
		exportDP();
	}
	
	private void exportDP() {
		CsvFileWriter csvFileWriter = null;
		DpRequirementExport dpRExport = new DpRequirementExport();
		
		try {
			csvFileWriter = getCSVWriter();
			csvFileWriter.writeNext(getHeader());
			
			boolean endOfDPs = false;
			int startAt = 0, maxRecs = 100;
		    
			while (!endOfDPs) {
      			int exportedRecsCount = exportDPs(csvFileWriter, startAt, maxRecs, dpRExport);
      			startAt += exportedRecsCount;
      			endOfDPs = (exportedRecsCount < maxRecs);
    		}
  		} catch (Exception e) {
  			logger.error("Error while running distribution protocol export job", e);
		} finally {
			dpRExport.closeWriter();
			IOUtils.closeQuietly(csvFileWriter);
		}
	}

	@PlusTransactional
	private int exportDPs(CsvFileWriter csvFileWriter, int startAt, int maxRecs, DpRequirementExport dprExport) throws IOException {
		DpListCriteria listCrit = new DpListCriteria().startAt(startAt).maxResults(maxRecs);
		List<DistributionProtocol> dPs = daoFactory.getDistributionProtocolDao().getDistributionProtocols(listCrit);

		for (DistributionProtocol dp : dPs) {
			writeToCsv(csvFileWriter, dp);
			exportDpr(dprExport, dp);
		}
		
		csvFileWriter.flush();
		dprExport.flushCsvWriter();
		return dPs.size();
	}

	private void writeToCsv(CsvFileWriter csvFileWriter, DistributionProtocol dp) {
		csvFileWriter.writeNext(getRow(dp));
	}

	private void exportDpr(DpRequirementExport dprExport, DistributionProtocol dp) {
		dprExport.exportDPRequirement(dp);
	}

	private CsvFileWriter getCSVWriter() {
		String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		File outputFile = new File(ConfigUtil.getInstance().getDataDir(), "DistributionProtocol_" + timestamp + ".csv");
		return CsvFileWriter.createCsvFileWriter(outputFile);
	}

	private String[] getHeader() {
		return new String[]{
			"TBR_REQUEST_TITLE",			// Title
			"TBR_SOURCE_REQUEST",			// Shorttitle
			"TBR_INSTITUTE_DESC",			// ReceivingInstitute
			"TBR_DEPT_DESC",			// Department
			"TBR_REQUESTER_DESC",			// PI#Email
			"TBR_REQUEST_DT",			// DPCustomFields#Request
			"TBR_ILAB_NO",				// DateDPCustomFields#ILABNo
			"TBR_FINALIZE_FLAG",			// DPCustomFields#FinalizeFlag
			"TBR_COST_CENTER",			// DPCustomFields#CostCenter
			"TBR_FUND_ID",				// DPCustomFields#FundID
			"TBR_MTA_FLAG",				// DPCustomFields#MTAFlag
			"TBR_DISTRIBUTION_OPTION_DESC",		// DPCustomFields#DistributionOptionDesc
			"TBR_HBC_ID",				// DPCustomFields#HBCID
			"TBR_HBC_COMMITTEE_APPROVAL_DT",	// DPCustomFields#HBCCommitteeApprovalDate
			"TBR_MIN_UNIT",				// DPCustomFields#MinimumUnit
			"TBR_MTA_APPROVAL_DT",			// DPCustomFields#MTAApprovalDate
			"TBR_PICKUP_ARRANGEMENT_DESC",		// DPCustomFields#PickupArrangementDescription
			"TBR_PROSPECT_FLAG",			// DPCustomFields#ProspectFlag
			"TBR_TYPE_DESC",			// DPCustomFields#TypeDescription
			"TBR_RESTROSPECT_FLAG",			// DPCustomFields#RestrospectFlag
			"TBR_SPECIMEN_COLLECTION_METHOD",	// DPCustomFields#SpecimenCollectionMethod
			"TBR_COMMENTS",				// DPCustomFields#Comments
			"TBR_CONTACT_NAME",			// DPCustomFields#ContactName
			"TBR_SPECIMEN_USAGE_DESC"		// DPCustomFields#SpecimenUsage
		};
	}

	private String[] getRow(DistributionProtocol dp) {
		List<String> row = new ArrayList<>();

		row.add(dp.getTitle());
		row.add(dp.getShortTitle());
		row.add(dp.getInstitute().getName());
		row.add(dp.getDefReceivingSite().getName());
		row.add(dp.getPrincipalInvestigator().getFirstName() + " " + dp.getPrincipalInvestigator().getLastName());
		row.addAll(getCustomFieldValues(dp));
		
		return row.toArray(new String[row.size()]);
	}
	
	private List<String> getCustomFieldValues(DistributionProtocol dp) {
		return dp.getExtension()
			.getAttrs().stream()
			.map(Attr::getDisplayValue)
			.collect(Collectors.toList());
	}
}	

