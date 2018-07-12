package com.krishagni.openspecimen;

import java.io.File;
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
		exportDistributionProtocol();
	}
	
	private void exportDistributionProtocol() {
		CsvFileWriter csvFileWriter = null;
		
		try {
			csvFileWriter = getCSVWriter();
			csvFileWriter.writeNext(getHeader());
			
			boolean endOfDistributionProtocol = false;
    		int startAt = 0, maxRecs = 100;
    		
    		while (!endOfDistributionProtocol) {
	      		int exportedRecsCount = exportDistributionProtocol(csvFileWriter, startAt, maxRecs);
	      		startAt += exportedRecsCount;
	      		endOfDistributionProtocol = (exportedRecsCount < maxRecs);
	    	}
  		} catch (Exception e) {
  			logger.error("Error while running distribution protocol export job", e);
		} finally {
			IOUtils.closeQuietly(csvFileWriter);
		}
	}

	@PlusTransactional
	private int exportDistributionProtocol(CsvFileWriter csvFileWriter, int startAt, int maxRecs) {
		List<DistributionProtocol> distributionProtocols = daoFactory.getDistributionProtocolDao().getDistributionProtocols(
				new DpListCriteria().startAt(startAt).maxResults(maxRecs)
				);
		distributionProtocols.forEach(dp -> csvFileWriter.writeNext(getRow(dp)));
		return distributionProtocols.size();
	}

	private CsvFileWriter getCSVWriter() {
		String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		return CsvFileWriter.createCsvFileWriter(
				new File(ConfigUtil.getInstance().getDataDir(), "DistributionProtocol_" + timeStamp + ".csv")
				);
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
