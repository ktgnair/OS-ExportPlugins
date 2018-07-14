package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

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
		DpRequirementExport dprExport = new DpRequirementExport();
		
		try {
			csvFileWriter = getCSVWriter();
			csvFileWriter.writeNext(getHeader());
			
			boolean endOfDPs = false;
			int startAt = 0, maxRecs = 100;
		    
			while (!endOfDPs) {
      			int exportedRecsCount = exportDPs(csvFileWriter, startAt, maxRecs, dprExport);
      			startAt += exportedRecsCount;
      			endOfDPs = (exportedRecsCount < maxRecs);
    		}
  		} catch (Exception e) {
  			logger.error("Error while running distribution protocol export job", e);
		} finally {
			dprExport.closeWriter();
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
		return new String[] {
				"TBR_REQUEST_TITLE", 
				"TBR_SOURCE_REQUEST", 
				"TBR_INSTITUTE_DESC", 
				"TBR_DEPT_DESC", 
				"TBR_REQUESTER_DESC"
		};
	}

	private String[] getRow(DistributionProtocol dp) {
		return new String[] { 
				dp.getTitle(), 
				dp.getShortTitle(), 
				dp.getInstitute().getName(), 
				dp.getDefReceivingSite().getName(), 
				dp.getPrincipalInvestigator().getFirstName() + " " + dp.getPrincipalInvestigator().getLastName()
		};
	}
}
