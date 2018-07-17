package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import com.krishagni.catissueplus.core.biospecimen.domain.Specimen;
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
        List<CollectionProtocolRegistration> cprs = daoFactory.getCprDao().getCprs(cprListCriteria);
        cprs.forEach(cpr -> writeCpr(cpr, csvFileWriter));
        csvFileWriter.flush();
        return cprs.size();
    }

    private void writeCpr(CollectionProtocolRegistration cpr, CsvFileWriter csvFileWriter) {
    	List<String> props = new ArrayList<>();
    	props.add(cpr.getParticipant().getEmpi());
    	
    	if (!cpr.getVisits().isEmpty()) {
                cpr.getVisits().forEach(visit -> writeVisit(visit, props, csvFileWriter));
    	} else {
                csvFileWriter.writeNext(props.toArray(new String[props.size()]));
    	}
    }

    private void writeVisit(Visit visit, List<String> visitProps, CsvFileWriter csvFileWriter) {
    	ArrayList<String> props = populateVisit(visit, visitProps);
	
    	if (!visit.getOrderedTopLevelSpecimens().isEmpty()) {
    	        visit.getOrderedTopLevelSpecimens().forEach(specimen -> writeSpecimen(specimen, props, csvFileWriter));
    	} else {
       	        csvFileWriter.writeNext(props.toArray(new String[props.size()]));
    	}
    }
	
    private void writeSpecimen(Specimen specimen, List<String> specimenProps, CsvFileWriter csvFileWriter) {
    	ArrayList<String> props = populateSpecimen(specimen, specimenProps);
		
    	csvFileWriter.writeNext(props.toArray(new String[props.size()]));
    }
	
    private ArrayList<String> populateVisit(Visit visit, List<String> visitProps) {
    	ArrayList<String> props = new ArrayList<String>(visitProps);
		
    	props.add(visit.getName());
    	props.add(visit.getVisitDate().toString()); 
    	props.add(visit.getSite().getName()); 
    	props.add(visit.getClinicalDiagnoses().iterator().next());
    	props.add(visit.getSurgicalPathologyNumber()); 
    	props.add(visit.getComments());
		
    	return props;
    }

    private ArrayList<String> populateSpecimen(Specimen specimen, List<String> specimenProps) {
    	ArrayList<String> props = new ArrayList<String>(specimenProps);
    	
    	props.add(specimen.getLabel());
    	props.add(specimen.getSpecimenType());
    	props.add(specimen.getTissueSite());
    	props.add(specimen.getTissueSide());
    	props.add(specimen.getPathologicalStatus());
    	props.add(specimen.getInitialQuantity().toString());
    	props.add(specimen.getCreatedOn().toString());
    	props.add(specimen.getComment());
    	props.add(specimen.getCollRecvDetails().getCollTime().toString());
    	props.add(specimen.getCollRecvDetails().getRecvTime().toString());
		
    	return props;
    }

    private CsvFileWriter getCSVWriter() {
        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        File file = new File(ConfigUtil.getInstance().getDataDir(), "participants_" + timeStamp + ".csv");
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
                "TBD_BANK_NOTE",
                "PARENT_SPECIMEN_LABEL",
                "TBD_SPECIMEN_TYPE_DESC",   
                "TBA_SITE_DESC",
                "TBA_SITE_SIDE_DESC",
                "TBD_CATEGORY_DESC",        
                "TBD_WEIGHT",
                "TBD_SAMPLE_PROCESS_DT",        
                "TBA_SITE_TEXT",        
                "TBA_RESECT_DT",      
                "TBA_BIOBANK_RECEIPT_DT"
        };
    }
}
