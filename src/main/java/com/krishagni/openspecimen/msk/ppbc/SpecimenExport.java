package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.domain.BaseExtensionEntity;
import com.krishagni.catissueplus.core.biospecimen.domain.Specimen;
import com.krishagni.catissueplus.core.biospecimen.repository.DaoFactory;
import com.krishagni.catissueplus.core.biospecimen.repository.SpecimenListCriteria;
import com.krishagni.catissueplus.core.common.PlusTransactional;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;
import com.krishagni.catissueplus.core.common.util.Utility;

@Configurable
public class SpecimenExport implements ScheduledTask {
    private static final Log logger = LogFactory.getLog(SpecimenExport.class);
	
    @Autowired
    private DaoFactory daoFactory;
	
    @Override
    public void doJob(ScheduledJobRun jobRun) {
        exportSpecimens();
    }

    private void exportSpecimens() {
        CsvFileWriter csvFileWriter = null;

        try {
            csvFileWriter = getCSVWriter();
            csvFileWriter.writeNext(getHeader());

            boolean endOfSpecimens = false;
            int startAt = 0, maxRecs = 100;

     	    while (!endOfSpecimens) {
               	int exportedRecsCount = exportSpecimens(csvFileWriter, startAt, maxRecs);
                startAt += exportedRecsCount;
                endOfSpecimens = (exportedRecsCount < maxRecs);
            }        
        } catch (Exception e) {
            logger.error("Error while running specimen export job", e);
        } finally {
            IOUtils.closeQuietly(csvFileWriter);
        }
    }
    
    private Map<String, String> getCustomFieldValueMap(BaseExtensionEntity obj) {
    	return obj.getExtension().getAttrs().stream().collect(
    			Collectors.toMap(
    				attr -> attr.getCaption(),
    				attr -> attr.getDisplayValue(""),
    				(v1, v2) -> {throw new IllegalStateException("Duplicate key");},
    				LinkedHashMap::new)
    			);
    }
    
    private CsvFileWriter getCSVWriter() {
        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        File file = new File(ConfigUtil.getInstance().getDataDir(), "specimens_" + timeStamp + ".csv");
        return CsvFileWriter.createCsvFileWriter(file);
    }
    
    @PlusTransactional
    private int exportSpecimens(CsvFileWriter csvFileWriter, int startAt, int maxRecs) throws IOException {
       	SpecimenListCriteria speciListCriteria = new SpecimenListCriteria()
       						.startAt(startAt)
       						.maxResults(maxRecs)
       						.lineages(new String[]{"Aliquot"});
       	List<Specimen> specimens = daoFactory.getSpecimenDao().getSpecimens(speciListCriteria);
        
       	specimens.forEach(specimen -> csvFileWriter.writeNext(getRow(specimen)));

       	csvFileWriter.flush();
       	return specimens.size();
    }
    
    private String[] getRow(Specimen specimen) {
    	List<String> row = new ArrayList<>();
    	
    	row.add(getPrimarySpecimen(specimen).getLabel());
    	row.add(specimen.getLabel());
    	row.add(specimen.getPathologicalStatus());
    	row.add(specimen.getSpecimenType());
    	row.add(getSequenceNumber(specimen));
    	row.add(getSpecimenQuantity(specimen, "TBD_VOL"));
    	row.add(getSpecimenQuantity(specimen, "TBD_WEIGHT"));
    	row.add(getSpecimenCreatedOn(specimen));
    	row.addAll(getCustomField(specimen));
    	
    	return row.toArray(new String[row.size()]);
    }
    
    private Specimen getPrimarySpecimen(Specimen specimen) {
    	if (specimen.getParentSpecimen().isPrimary()) {
    		return specimen.getParentSpecimen();
    	}
    	
    	return getPrimarySpecimen(specimen.getParentSpecimen());
    }
    
    private String getSequenceNumber(Specimen specimen) {
    	String specimenLabel = specimen.getLabel();
    	String[] output = specimenLabel.split("\\.");
   	     
    	return output[1];
    }
    
    private String getSpecimenQuantity(Specimen specimen, String columnName) {
    	if (specimen.getSpecimenClass().equals("Tissue") && columnName == "TBD_WEIGHT") {
    		return specimen.getAvailableQuantity().toString();
    	} else if (!specimen.getSpecimenClass().equals("Tissue") && columnName == "TBD_VOL") {
    		return specimen.getAvailableQuantity().toString();
    	} else {
    		return null;
    	}
    }	
    
    private String getSpecimenCreatedOn(Specimen specimen) {
    	return specimen.getCreatedOn() != null ? Utility.getDateTimeString(specimen.getCreatedOn()) : "";
    }
    
    private List<String> getCustomField(Specimen specimen) {
    	ArrayList<String> row = new ArrayList<String>();
    	Map<String, String> customFieldValueMap = getCustomFieldValueMap(specimen);
    	
    	row.add((String) (customFieldValueMap.getOrDefault("Freshness Degree","")));
    	row.add(getCalculatedTime(specimen, customFieldValueMap.getOrDefault("Time Lapse", "")));
    	row.add((String) (customFieldValueMap.getOrDefault("Unit Description","")));
    	row.add((String) (customFieldValueMap.getOrDefault("Special Handling Description","")));
    	row.add((String) (customFieldValueMap.getOrDefault("Is The Sample Sterile?","")));
    	row.add((String) (customFieldValueMap.getOrDefault("Biobank Technician","")));
    	row.add((String) (customFieldValueMap.getOrDefault("Additional Information","")));
    	row.add((String) (customFieldValueMap.getOrDefault("Additional Processing Date","")));
    	row.add((String) (customFieldValueMap.getOrDefault("Additional Processing Technician","")));
    	row.add((String) (customFieldValueMap.getOrDefault("Additional Processing Temperature","")));
   
    	return row;
    }
    
    private String getCalculatedTime(Specimen specimen, String timeLapse) {
    	
    	Date collDate = getPrimarySpecimen(specimen).getCollRecvDetails().getCollTime();
    	Date createdDate = specimen.getCreatedOn();
    	
    	long diff = createdDate.getTime() - collDate.getTime();
    	long timeInMinutes = TimeUnit.MILLISECONDS.toMinutes(diff);
    	
    	return timeLapse.equals("") ? Long.toString(timeInMinutes) : timeLapse;
    }

    private String[] getHeader() {
        return new String[] {
     	    	"PARENT_SPECIMEN_LABEL",
        	"ALIQUOT_LABEL",
        	"TBD_CATEGORY_DESC",
        	"TBD_SPECIMEN_TYPE_DESC",
        	"TBD_BANK_SEQ_NUM",
        	"TBD_VOL",
        	"TBD_WEIGHT",
        	"TBD_SAMPLE_PROCESS_DT", 
        	"TBD_QUALITY_DESC",
        	"TBD_TIME_LAPSE_MIN",
        	"TBD_UNIT_DESC",
        	"TBD_SPECIAL_HANDLING_DESC",
        	"TBD_STERILE_CODE_DESC",
        	"TBD_BIOBANK_TECH_NAME",
        	"TBD_ADDTL_DETAILS",
        	"TBD_ADDTL_PROCESS_DT",
        	"TBD_ADDTL_PROCESS_TECH_NAME",
        	"TBD_ADDTL_PROCESS_TEMPERATURE_DESC"
       	};
    }
}
