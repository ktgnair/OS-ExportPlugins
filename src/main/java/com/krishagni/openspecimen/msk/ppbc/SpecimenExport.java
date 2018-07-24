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

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.domain.Specimen;
import com.krishagni.catissueplus.core.biospecimen.repository.DaoFactory;
import com.krishagni.catissueplus.core.biospecimen.repository.SpecimenListCriteria;
import com.krishagni.catissueplus.core.common.PlusTransactional;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;
import com.krishagni.catissueplus.core.de.domain.DeObject.Attr;

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
    
    @PlusTransactional
    private int exportSpecimens(CsvFileWriter csvFileWriter, int startAt, int maxRecs) throws IOException {
       	SpecimenListCriteria speciListCriteria = new SpecimenListCriteria().startAt(startAt).maxResults(maxRecs).lineages(new String[]{"Aliquot"});
       	List<Specimen> specimens = daoFactory.getSpecimenDao().getSpecimens(speciListCriteria);
        
       	specimens.forEach(specimen -> csvFileWriter.writeNext(getRow(specimen)));

       	csvFileWriter.flush();
       	return specimens.size();
    }
    
    private String[] getRow(Specimen specimen) {
    	List<String> row = new ArrayList<>();
    	getCustomFieldNames(specimen);
    	
    	row.add(getPrimarySpecimenLabel(specimen));
    	row.add(specimen.getLabel());
    	row.add(specimen.getPathologicalStatus());
    	row.add(specimen.getSpecimenType());
    	row.add(getSequenceNumber(specimen));
    	row.add(getSpecimenQuantity(specimen, "TBD_VOL"));
    	row.add(getSpecimenQuantity(specimen, "TBD_WEIGHT"));
    	row.add(specimen.getCreatedOn().toString());
    	row.addAll(getCustomField(specimen));
    	
    	return row.toArray(new String[row.size()]);
    }
    
    private String getPrimarySpecimenLabel(Specimen specimen) {
    	if (specimen.getParentSpecimen().isPrimary()) {
    		return specimen.getParentSpecimen().getLabel();
    	}
    	
    	return getPrimarySpecimenLabel(specimen.getParentSpecimen());
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
    
    private List<String> getCustomFieldValues(Specimen specimen) {
    	return specimen.getExtension()
    		.getAttrs().stream()
    		.map(Attr::getDisplayValue)
    		.collect(Collectors.toList());
    }
    
    private List<String> getCustomFieldNames(Specimen specimen) {
   	return specimen.getExtension()
    		.getAttrs().stream()
    		.map(Attr::getCaption)
    		.collect(Collectors.toList());
    }
    
    private List<String> getCustomField(Specimen specimen) {
    	List<String> customList = getCustomFieldNames(specimen);
    	List<String> valueList = getCustomFieldValues(specimen);
		
       	ArrayList<String> row = new ArrayList<String>();
        row.add(valueList.get(customList.indexOf("Freshness Degree")));
        row.add(valueList.get(customList.indexOf("Time Lapse")));
        row.add(valueList.get(customList.indexOf("Unit Description")));
        row.add(valueList.get(customList.indexOf("Special Handling Description")));
        row.add(valueList.get(customList.indexOf("Is The Sample Sterile?")));
        row.add(valueList.get(customList.indexOf("Biobank Technician")));
        row.add(valueList.get(customList.indexOf("Additional Information")));
        row.add(valueList.get(customList.indexOf("Additional Processing Date")));
        row.add(valueList.get(customList.indexOf("Additional Processing Technician")));
        row.add(valueList.get(customList.indexOf("Additional Processing Temperature")));

    	return row;
    }
    
    private CsvFileWriter getCSVWriter() {
        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        File file = new File(ConfigUtil.getInstance().getDataDir(), "specimens_" + timeStamp + ".csv");
        return CsvFileWriter.createCsvFileWriter(file);
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
        	"TBD_ADDTL_PROCESS_TEMPERATURE_DESC", 		
       	};
    }
}
