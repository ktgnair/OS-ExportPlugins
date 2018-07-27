package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.domain.BaseExtensionEntity;
import com.krishagni.catissueplus.core.biospecimen.domain.CollectionProtocolRegistration;
import com.krishagni.catissueplus.core.biospecimen.domain.Specimen;
import com.krishagni.catissueplus.core.biospecimen.domain.Visit;
import com.krishagni.catissueplus.core.biospecimen.repository.CprListCriteria;
import com.krishagni.catissueplus.core.biospecimen.repository.DaoFactory;
import com.krishagni.catissueplus.core.common.PlusTransactional;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;
import com.krishagni.catissueplus.core.de.domain.DeObject.Attr;

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
    	props.add((String) (getCustomFieldValueMap(cpr.getParticipant()).getOrDefault("NT3", "")));
    	
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
    	props.add(getSiteName(visit));
    	props.add(getClinicalDiagnoses(visit));
    	props.add(visit.getSurgicalPathologyNumber()); 
    	props.add(visit.getComments());
    	props.addAll(getCustomField(visit));
		
    	return props;
    }
    
    private String getSiteName(Visit visit) {
    	return visit.getSite() != null ? visit.getSite().getName() : "";
    }
    
    private String getClinicalDiagnoses(Visit visit) {
    	return visit.getClinicalDiagnoses().isEmpty() ? "" : visit.getClinicalDiagnoses().iterator().next();
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
    	props.addAll(getCustomField(specimen));

    	return props;
    }
    
    private Map<String, Object> getCustomFieldValueMap(BaseExtensionEntity obj) {
	return obj.getExtension().getAttrValues();
    }
    
    private List<String> getCustomFieldValues(BaseExtensionEntity obj) {
	return obj.getExtension()
		.getAttrs().stream()
		.map(Attr::getDisplayValue)
		.collect(Collectors.toList());
    }
    
    private List<String> getCustomFieldNames(BaseExtensionEntity obj) {
       	return obj.getExtension()
        	.getAttrs().stream()
        	.map(Attr::getCaption)
        	.collect(Collectors.toList());
    }
      
    private List<String> getCustomField(Visit visit) {
        List<String> customList = getCustomFieldNames(visit);
        List<String> valueList = getCustomFieldValues(visit);
    		
        ArrayList<String> row = new ArrayList<String>();
        row.add(valueList.get(customList.indexOf("Diagnosis Notes")));
        row.add(valueList.get(customList.indexOf("Operation Date")));
        row.add(valueList.get(customList.indexOf("Path Date")));
        row.add(valueList.get(customList.indexOf("Surgeon Name")));
        row.add(valueList.get(customList.indexOf("Specimen Description")));
        row.add(valueList.get(customList.indexOf("NUN (N)")));
        row.add(valueList.get(customList.indexOf("NUN (T)")));
        row.add(valueList.get(customList.indexOf("OCT (N)")));
        row.add(valueList.get(customList.indexOf("OCT (T)")));

       	return row;
    }
    
    private List<String> getCustomField(Specimen specimen) {
    	ArrayList<String> row = new ArrayList<String>();
    	List<String> customList = getCustomFieldNames(specimen);
    	List<String> valueList = getCustomFieldValues(specimen);
       	
        row.add(valueList.get(customList.indexOf("Part Number")));
        row.add(valueList.get(customList.indexOf("Part Sub Number")));
        row.add(valueList.get(customList.indexOf("Biobank Technician")));
        row.add(valueList.get(customList.indexOf("Accessioning Temperature Condition")));
        row.add(valueList.get(customList.indexOf("Biobank Temperature")));
        row.add(valueList.get(customList.indexOf("Location")));
       
    	return row;
    }

    private CsvFileWriter getCSVWriter() {
        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        File file = new File(ConfigUtil.getInstance().getDataDir(), "participants_" + timeStamp + ".csv");
        return CsvFileWriter.createCsvFileWriter(file);
    }

    private String[] getHeader() {
        return new String[] {
                "TBA_CRDB_MRN",
                "TBA_PT_DEIDENTIFICATION_ID",
                "TBD_BANK_NUM",
                "TBA_PROCUREMENT_DTE",
                "TBD_BANK_SUB_CD",
                "TBA_DISEASE_DESC",
                "TBA_ACCESSION_NUM",
                "TBD_BANK_NOTE",
                "TBA_DIAGNOSIS_NOTE",
                "TBA_SURG_STRT_DT",
                "TBA_PATH_REVIEW_DT",
                "TBA_SURGEON_NAME",
                "Surgical path report",
                "TBD_NUN_N",
                "TBD_NUN_T",
                "TBD_OCT_N",
                "TBD_OCT_T",
                "PARENT_SPECIMEN_LABEL",
                "TBD_SPECIMEN_TYPE_DESC",   
                "TBA_SITE_DESC",
                "TBA_SITE_SIDE_DESC",
                "TBD_CATEGORY_DESC",        
                "TBD_WEIGHT",
                "TBD_SAMPLE_PROCESS_DT",        
                "TBA_SITE_TEXT",        
                "TBA_RESECT_DT",      
                "TBA_BIOBANK_RECEIPT_DT",
                "TBA_PART_NUM",
                "TBA_SUB_PART_NUM",
                "TBA_BIOBANK_TECH_NAME",
                "TBA_TEMPERATURE_COND_DESC",
                "TBA_BIOBANK_TEMPERATURE_COND_DESC",
                "TBA_SITE_LOCATION_DESC"
        };
    }
}
