package com.krishagni.openspecimen.msk.ppbc;

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;

public class ExportJobDriver implements ScheduledTask {

	@Override
	public void doJob(ScheduledJobRun jobRun) throws Exception {
		runExportJobs(jobRun);
	}

	private void runExportJobs(ScheduledJobRun jobRun) {
		DistributionProtocolExport DpExport = new DistributionProtocolExport();
		SpecimenExport specimenExport = new SpecimenExport();
		ParticipantExport participantExport = new ParticipantExport();
		
		DpExport.doJob(jobRun);
		specimenExport.doJob(jobRun);
		participantExport.doJob(jobRun);
	}

}
