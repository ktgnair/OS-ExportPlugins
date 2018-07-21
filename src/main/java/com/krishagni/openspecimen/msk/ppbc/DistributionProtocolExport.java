package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import com.krishagni.catissueplus.core.administrative.domain.DistributionOrder;
import com.krishagni.catissueplus.core.administrative.domain.DistributionOrderItem;
import com.krishagni.catissueplus.core.administrative.domain.DistributionProtocol;
import com.krishagni.catissueplus.core.administrative.domain.DpRequirement;
import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.repository.DpListCriteria;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.domain.BaseExtensionEntity;
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
		export();
	}
	
	private void export() {
		CsvFileWriter dpFileWriter = null, dPRFileWriter = null, doFileWriter = null;
		
		try {
			dpFileWriter = getDpCSVWriter();
			dPRFileWriter = getDpRCSVWriter();
			doFileWriter = getDoCSVWriter();
			
			dpFileWriter.writeNext(getDpHeader());
			dPRFileWriter.writeNext(getDpRHeader());
			doFileWriter.writeNext(getDoHeader());
			
			boolean endOfDPs = false;
			int startAt = 0, maxRecs = 100;
		    
			while (!endOfDPs) {
      			int exportedRecsCount = exportDpData(dpFileWriter, dPRFileWriter, doFileWriter, startAt, maxRecs);
      			startAt += exportedRecsCount;
      			endOfDPs = (exportedRecsCount < maxRecs);
    		}
  		} catch (Exception e) {
  			logger.error("Error while running distribution protocol export job", e);
		} finally {
			IOUtils.closeQuietly(dPRFileWriter);
			IOUtils.closeQuietly(dpFileWriter);
			IOUtils.closeQuietly(doFileWriter);
		}
	}

	@PlusTransactional
	private int exportDpData(CsvFileWriter dpFileWriter, CsvFileWriter dPRFileWriter, CsvFileWriter doFileWriter, int startAt, int maxRecs) throws IOException {
		DpListCriteria listCrit = new DpListCriteria().startAt(startAt).maxResults(maxRecs);
		List<DistributionProtocol> dPs = daoFactory.getDistributionProtocolDao().getDistributionProtocols(listCrit);
		
		exportDp(dpFileWriter, dPs);
		dPs.forEach(dp -> exportDpr(dPRFileWriter, dp.getRequirements()));
		dPs.forEach(dp -> exportDOs(doFileWriter, dp.getDistributionOrders()));
		
		dpFileWriter.flush();
		dPRFileWriter.flush();
		doFileWriter.flush();

		return dPs.size();
	}
	
	private List<String> getCustomFieldValues(BaseExtensionEntity obj) {
		return obj.getExtension()
			.getAttrs().stream()
			.map(Attr::getDisplayValue)
			.collect(Collectors.toList());
	}

	///////////////////////
	//
	// DP export
	//
	///////////////////////

	private void exportDp(CsvFileWriter dpFileWriter, List<DistributionProtocol> dPs) {
		dPs.forEach(dp -> dpFileWriter.writeNext(getDpRow(dp)));
	}

	private CsvFileWriter getDpCSVWriter() {
		String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		File outputFile = new File(ConfigUtil.getInstance().getDataDir(), "DistributionProtocol_" + timestamp + ".csv");
		return CsvFileWriter.createCsvFileWriter(outputFile);
	}

	private String[] getDpHeader() {
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

	private String[] getDpRow(DistributionProtocol dp) {
		List<String> row = new ArrayList<>();

		row.add(dp.getTitle());
		row.add(dp.getShortTitle());
		row.add(dp.getInstitute().getName());
		row.add(dp.getDefReceivingSite().getName());
		row.add(dp.getPrincipalInvestigator().getFirstName() + " " + dp.getPrincipalInvestigator().getLastName());
		row.addAll(getCustomFieldValues(dp));
		
		return row.toArray(new String[row.size()]);
	}
	
	///////////////////////
	//
	// DPR export
	//
	///////////////////////
	
	private void exportDpr(CsvFileWriter dpFileWriter, Set<DpRequirement> DpRequirements) {
		if (!DpRequirements.isEmpty()) {
			DpRequirements.forEach(dPR -> dpFileWriter.writeNext(getDpRRow(dPR)));
		}
	}

	private String[] getDpRRow(DpRequirement dpr) {
		return new String[] {
			dpr.getSpecimenType(),
			dpr.getAnatomicSite(),
			dpr.getPathologyStatuses().iterator().next(),
			dpr.getQuantity().toString(),
			dpr.getCost().toString(),
			dpr.getDistributionProtocol().getShortTitle()
		};
	}

	private CsvFileWriter getDpRCSVWriter() {
		String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		File outputFile = new File(ConfigUtil.getInstance().getDataDir(), "DpRequirement_" + timestamp + ".csv");
		return CsvFileWriter.createCsvFileWriter(outputFile);
	}

	private String[] getDpRHeader() {
		return new String[] {
				"TBRD_SPECIMEN_TYPE_CD",
				"TBRD_SITE_DESC",
				"TBRD_CATEGORY_DESC",
				"TBRD_EXPECTED_AMT", 
				"TBRD_BILLING_AMT",
				"TBRD_SOURCE_REQUEST"
		};
	}
	
	///////////////////////
	//
	// Distribution Orders
	//
	///////////////////////
	
	private CsvFileWriter getDoCSVWriter() {
		String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		File outputFile = new File(ConfigUtil.getInstance().getDataDir(), "DistributionOrders_" + timestamp + ".csv");
		return CsvFileWriter.createCsvFileWriter(outputFile);
	}
	
	private String[] getDoHeader() {
		return new String[] {
				"Name",
				"TBDS_DISTRIBUTION_DT",
				"TBDS_SOURCE_REQUEST",
				"TBDS_BILLING_AMT",
				"Specimen Label",
				"TBDS_BILLING_DT"
		};
	}

	private void exportDOs(CsvFileWriter doFileWriter, Set<DistributionOrder> distributionOrders) {
		if (!distributionOrders.isEmpty()) {
			distributionOrders.forEach(distributionOrder -> writeDoToCsv(doFileWriter, distributionOrder.getOrderItems()));
		}
	}
	
	private void writeDoToCsv(CsvFileWriter doFileWriter, Set<DistributionOrderItem> orderItems) {
		if (!orderItems.isEmpty()) {
			orderItems.forEach(item -> doFileWriter.writeNext(getDoRow(item)));
		}
	}

	private String[] getDoRow(DistributionOrderItem item) {
		List<String> row = new ArrayList<String>();
		
		row.add(item.getOrder().getName());
		row.add(item.getOrder().getExecutionDate().toString());
		row.add(item.getOrder().getDistributionProtocol().getShortTitle());
		row.add(getItemCost(item));
		row.add(item.getSpecimen().getLabel());
		row.addAll(getCustomFieldValues(item.getOrder()));
		
		return row.toArray(new String[row.size()]);
	}

	private String getItemCost(DistributionOrderItem item) {
		if (item.getCost() != null) {
			return item.getCost().toString();
		} else {
			return "";
		}
	}
}	
