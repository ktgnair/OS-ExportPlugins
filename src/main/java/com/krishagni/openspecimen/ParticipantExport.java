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

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.domain.CollectionProtocolRegistration;
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
			List<Long> cpIds = getAllCpIds();
			
			for (Long cpId : cpIds) {
				boolean endOfParticipants = false;
		    		int startAt = 0, maxRecs = 100;
		    		
		    		while (!endOfParticipants) {
	    	      			int exportedRecsCount = exportParticipants(csvFileWriter, cpId, startAt, maxRecs);
	 		      		startAt += exportedRecsCount;
	 		      		endOfParticipants = (exportedRecsCount < maxRecs);
	    	    		}
			}
  		} catch (Exception e) {
  			logger.error("Error while running participant export job", e);
		} finally {
			IOUtils.closeQuietly(csvFileWriter);
		}
	}
	
	@PlusTransactional
	private List<Long> getAllCpIds() {
		return daoFactory.getCollectionProtocolDao().getAllCpIds();
	}

	@PlusTransactional
	private int exportParticipants(CsvFileWriter csvFileWriter, Long cpId, int startAt, int maxRecs) {
		List<CollectionProtocolRegistration> cprs = daoFactory.getCprDao().getCprs(
				new CprListCriteria().cpId(cpId).startAt(startAt).maxResults(maxRecs)
				);
		cprs.forEach(cpr -> csvFileWriter.writeNext(getRow(cpr)));
		return cprs.size();
	}

	private CsvFileWriter getCSVWriter() {
		String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		return CsvFileWriter.createCsvFileWriter(
				new File(ConfigUtil.getInstance().getDataDir(), "participants_" + timeStamp + ".csv")
				);
	}

	private String[] getHeader() {
		return new String[] {"firstName", "lastName"};
	}

	private String[] getRow(CollectionProtocolRegistration cprDetail) {
		return new String[] {cprDetail.getParticipant().getFirstName(), cprDetail.getParticipant().getLastName()};
	}
}
