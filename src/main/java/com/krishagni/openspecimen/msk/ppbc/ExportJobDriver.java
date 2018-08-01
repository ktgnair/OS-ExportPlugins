package com.krishagni.openspecimen.msk.ppbc;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.SftpUtil;
import com.krishagni.catissueplus.core.common.util.SshSession;

public class ExportJobDriver implements ScheduledTask {
	private static final Log logger = LogFactory.getLog(ExportJobDriver.class);
	
	private final static String dbDataDir = "/usr/local/var/mysql";
	private final static String remoteHost = "";
	private final static String remoteUsername = "";
	private final static String remotePassword = "";
	
	private ScheduledTask[] tasks = {
			   new DistributionProtocolExport(),
			   new SpecimenExport(),
			   new ParticipantExport()
			};
	
	@Override
	public void doJob(ScheduledJobRun jobRun) throws Exception {
		for (ScheduledTask task : tasks) {
			task.doJob(jobRun);
		}
		
		importCSVToDb("DistributionProtocol_20180728205009.csv", "request_from_java");
		importCSVToDb("DpRequirement_20180728205009.csv", "dp_requirement_from_java");
		importCSVToDb("DistributionOrders_20180728205009.csv", "dp_order_from_java");
		importCSVToDb("participants_20180728205011.csv", "participant_from_java");
		importCSVToDb("specimens_20180728205010.csv", "specimen_from_java");
	}

	private void importCSVToDb(String csvFileName, String dbTableName) {
		ensureCSVIsAccessible(csvFileName);
		
		SingleConnectionDataSource scds = new SingleConnectionDataSource("jdbc:mysql://localhost:3306/loadfromcsv","swapnil","root", true);
		JdbcTemplate jdbcTemplate = new JdbcTemplate(scds);
		jdbcTemplate.execute("LOAD DATA INFILE " + "'" + dbDataDir + File.separatorChar + csvFileName + "'\n" + 
				"IGNORE INTO TABLE "+ dbTableName + "\n" + 
				"FIELDS TERMINATED BY ',' ENCLOSED BY '\"'\n" + 
				"LINES TERMINATED BY '\\n'\n" + 
				"IGNORE 1 LINES;");
		
		scds.destroy();
	}
	
	private void ensureCSVIsAccessible(String sourceFileName)  {
		File destination = new File(dbDataDir);
		File source = new File(ConfigUtil.getInstance().getDataDir(), sourceFileName);
		
		if (StringUtils.isEmpty(remoteHost) || StringUtils.isEmpty(remoteUsername) || StringUtils.isEmpty(remotePassword)) {
		    try {
				FileUtils.copyFileToDirectory(source, destination);
			} catch (IOException e) {
				logger.error("Error while copying csv file from source to destination directory", e);
			}
		} else {
		    putFileOnRemote(source.getAbsolutePath(), destination.getAbsolutePath());
		}
	}
	
	private void putFileOnRemote(String localPath, String remotePath) {
		SshSession ssh = new SshSession(remoteHost, remoteUsername, remotePassword);
		ssh.connect();

		SftpUtil sftp = ssh.newSftp();
		sftp.put(localPath, remotePath);

		sftp.close();
		ssh.close();
	}
}
