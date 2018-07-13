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

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.domain.CollectionProtocolRegistration;
import com.krishagni.catissueplus.core.biospecimen.domain.Visit;
import com.krishagni.catissueplus.core.biospecimen.repository.CprListCriteria;
import com.krishagni.catissueplus.core.biospecimen.repository.DaoFactory;
import com.krishagni.catissueplus.core.common.PlusTransactional;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;

@Configurable
public class ParticipantExport implements ScheduledTask {
	private static final Log logger = LogFactory.getLog(ParticipantExport.class);

	@Autowired
	private DaoFactory daoFactory;
		
	@Override
	public void doJob(ScheduledJobRun jobRun) {
		exportParticipants();
	}
	
	private void exportParticipants() {
		CsvFileWriter csvFileWriter = null;
		
		try {
			csvFileWriter = getCSVWriter();
			csvFileWriter.writeNext(getHeader());
				
			boolean endOfParticipants = false;
		        int startAt = 0, maxRecs = 100;
		    		
	    		while (!endOfParticipants) {
	      			int exportedRecsCount = exportParticipants(csvFileWriter, startAt, maxRecs);
	      			startAt += exportedRecsCount;
	      			endOfParticipants = (exportedRecsCount < maxRecs);
    	    		}		
  		} catch (Exception e) {
  			logger.error("Error while running participant export job", e);
		} finally {
			IOUtils.closeQuietly(csvFileWriter);
		}
	}

	@PlusTransactional
	private int exportParticipants(CsvFileWriter csvFileWriter, int startAt, int maxRecs) throws IOException {
		CprListCriteria cprListCriteria = new CprListCriteria().startAt(startAt).maxResults(maxRecs);
		List<CollectionProtocolRegistration> cprs = daoFactory.getCprDao().getCprs(cprListCriteria);;
		
		cprs.forEach(cpr -> {
			if (cpr.getVisits().isEmpty()) {
				csvFileWriter.writeNext(getRow(cpr));
			} else {
				cpr.getVisits().forEach(visit -> csvFileWriter.writeNext(getRow(cpr, visit)));
			}
		});
		
		csvFileWriter.flush();
		return cprs.size();
	}
	
	private String[] getRow(CollectionProtocolRegistration cpr, Visit visit) {
		return new String[] {
				cpr.getParticipant().getEmpi(),
				visit.getName(),
				visit.getVisitDate().toString(),
				visit.getSite().getName(),
				visit.getClinicalDiagnoses().iterator().next(),
				visit.getSurgicalPathologyNumber(),
				visit.getComments()
		};
	}
	
	private String[] getRow(CollectionProtocolRegistration cpr) {
		return new String[] {cpr.getParticipant().getEmpi()};
	}

	private CsvFileWriter getCSVWriter() {
		String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		File file =	new File(ConfigUtil.getInstance().getDataDir(), "participants_" + timeStamp + ".csv");
		return CsvFileWriter.createCsvFileWriter(file);
	}

	private String[] getHeader() {
		return new String[] {
				"TBA_CRDB_MRN", 
				"TBD_BANK_NUM", 
				"TBA_PROCUREMENT_DTE", 
				"TBD_BANK_SUB_CD",
				"TBA_DISEASE_DESC",
				"TBA_ACCESSION_NUM",
				"TBD_BANK_NOTE"
		};
	}
}
