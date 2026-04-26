/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useContext, useEffect, useState } from 'react';
import {
  Grid,
  makeStyles,
  Typography,
  Box,
  Paper,
  MenuItem,
  Select,
  TextField,
  FormControl,
  InputLabel
} from '@material-ui/core';
import { get } from 'lodash';
import { TableData } from 'Models';
import useTaskTypesTable from '../components/Homepage/useTaskTypesTable';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomButton from '../components/CustomButton';
import CustomDialog from '../components/CustomDialog';
import CustomizedTables from '../components/Table';
import { NotificationContext } from '../components/Notification/NotificationContext';
import { formatTimeInTimezone } from '../utils/TimezoneUtils';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto'
  },
  summaryPaper: {
    padding: '12px 0',
    textAlign: 'center',
    color: '#4285f4',
    backgroundColor: 'rgba(66, 133, 244, 0.08)',
    border: '1px solid rgba(66, 133, 244, 0.4)',
    borderRadius: 4,
    '& h2, h4': {
      margin: 0,
    },
    '& h4': {
      textTransform: 'uppercase',
      letterSpacing: 1,
      fontWeight: 600,
      fontSize: 12,
    },
    '& h2': {
      fontSize: 28,
    },
  },
  schedulerBox: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20,
  },
  schedulerBody: {
    padding: '12px 16px',
    fontSize: 14,
    lineHeight: '2rem',
  },
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20
  },
  formField: {
    width: '100%',
  }
}));

type SchedulerInfo = {
  Version?: string;
  SchedulerName?: string;
  SchedulerInstanceId?: string;
  InStandbyMode?: boolean;
  RunningSince?: number;
  NumberOfJobsExecuted?: number;
  JobDetails?: Array<unknown>;
  // The controller currently emits the leaked Java method name `getThreadPoolSize`
  // (see PinotTaskRestletResource.getCronSchedulerInformation). We tolerate the
  // cleaner key name here as a forward-compatibility hedge.
  getThreadPoolSize?: number;
  threadPoolSize?: number;
};

const MinionTaskManager = () => {
  const classes = useStyles();
  const { dispatch } = useContext(NotificationContext);
  const { taskTypes, taskTypesTable } = useTaskTypesTable();

  const [periodicTaskNames, setPeriodicTaskNames] = useState<TableData>({ records: [], columns: ['Periodic Task Name'] });
  const [schedulerInfo, setSchedulerInfo] = useState<SchedulerInfo | null>(null);
  const [minionCount, setMinionCount] = useState<number | null>(null);

  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const [selectedPeriodicTask, setSelectedPeriodicTask] = useState<string>('');
  const [periodicTaskTableName, setPeriodicTaskTableName] = useState<string>('');
  const [periodicTaskTableType, setPeriodicTaskTableType] = useState<string>('');
  const [running, setRunning] = useState(false);

  // Each section is fetched independently so that one slow / failed call does not
  // black out the whole page. The controller can transiently 5xx individual
  // endpoints (e.g., scheduler info when scheduler is disabled), and we want the
  // rest of the dashboard to keep working.
  const refresh = async () => {
    const results = await Promise.allSettled([
      PinotMethodUtils.getAllPeriodicTaskNames(),
      PinotMethodUtils.getCronSchedulerInformationData(),
      PinotMethodUtils.getAllInstances()
    ]);

    const [periodicResult, schedulerResult, instancesResult] = results;

    if (periodicResult.status === 'fulfilled') {
      setPeriodicTaskNames(periodicResult.value);
    } else {
      console.warn('Failed to load periodic task names', periodicResult.reason);
    }

    if (schedulerResult.status === 'fulfilled') {
      setSchedulerInfo(schedulerResult.value as SchedulerInfo | null);
    } else {
      console.warn('Failed to load cron scheduler info', schedulerResult.reason);
      setSchedulerInfo(null);
    }

    if (instancesResult.status === 'fulfilled') {
      setMinionCount(get(instancesResult.value, 'MINION', []).length);
    } else {
      console.warn('Failed to load instances', instancesResult.reason);
    }
  };

  useEffect(() => {
    refresh();
    // refresh has no closure-captured deps that change between renders.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleRunPeriodicTask = async () => {
    if (!selectedPeriodicTask) {
      return;
    }
    setRunning(true);
    try {
      const response = await PinotMethodUtils.runPeriodicTaskAction(
        selectedPeriodicTask,
        periodicTaskTableName.trim() || undefined,
        periodicTaskTableType.trim() || undefined
      );
      dispatch({
        type: 'success',
        message: get(response, 'description') || `Periodic task ${selectedPeriodicTask} triggered`,
        show: true,
      });
      setRunDialogOpen(false);
      setPeriodicTaskTableName('');
      setPeriodicTaskTableType('');
      setSelectedPeriodicTask('');
    } catch (e) {
      dispatch({
        type: 'error',
        message: `Failed to trigger periodic task: ${get(e, 'response.data.error') || (e as Error).message}`,
        show: true,
      });
    } finally {
      setRunning(false);
    }
  };

  const taskTypeCount = taskTypes?.records?.length || 0;
  const periodicTaskCount = periodicTaskNames?.records?.length || 0;
  const jobsExecuted = schedulerInfo?.NumberOfJobsExecuted ?? '-';
  const schedulerStatus = schedulerInfo
    ? (schedulerInfo.InStandbyMode ? 'Standby' : 'Running')
    : 'Disabled';
  const runningSince = schedulerInfo?.RunningSince
    ? formatTimeInTimezone(schedulerInfo.RunningSince, 'YYYY-MM-DD HH:mm:ss z')
    : '-';

  return (
    <Grid item xs className={classes.gridContainer}>
      <Box mb={2}>
        <Typography variant='h5'>Minion Task Manager</Typography>
        <Typography variant='caption'>Manage minion task queues, trigger tasks, and inspect cron scheduler state</Typography>
      </Box>

      <Grid container spacing={2}>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Task Types</h4>
            <h2>{taskTypeCount}</h2>
          </Paper>
        </Grid>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Minion Instances</h4>
            <h2>{minionCount === null ? '-' : minionCount}</h2>
          </Paper>
        </Grid>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Periodic Tasks</h4>
            <h2>{periodicTaskCount}</h2>
          </Paper>
        </Grid>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Cron Scheduler</h4>
            <h2>{schedulerStatus}</h2>
          </Paper>
        </Grid>
      </Grid>

      <Box mt={3} />

      <div className={classes.schedulerBox}>
        <SimpleAccordion headerTitle="Cron Scheduler Information" showSearchBox={false}>
          <Grid container className={classes.schedulerBody}>
            <Grid item xs={6}><strong>Scheduler Name:</strong> {schedulerInfo?.SchedulerName || '-'}</Grid>
            <Grid item xs={6}><strong>Instance Id:</strong> {schedulerInfo?.SchedulerInstanceId || '-'}</Grid>
            <Grid item xs={6}><strong>Version:</strong> {schedulerInfo?.Version || '-'}</Grid>
            <Grid item xs={6}><strong>Thread Pool Size:</strong> {schedulerInfo?.getThreadPoolSize ?? schedulerInfo?.threadPoolSize ?? '-'}</Grid>
            <Grid item xs={6}><strong>Jobs Executed:</strong> {jobsExecuted}</Grid>
            <Grid item xs={6}><strong>Running Since:</strong> {runningSince}</Grid>
            <Grid item xs={6}><strong>State:</strong> {schedulerStatus}</Grid>
            <Grid item xs={6}><strong>Scheduled Jobs:</strong> {schedulerInfo?.JobDetails?.length ?? '-'}</Grid>
          </Grid>
        </SimpleAccordion>
      </div>

      <div className={classes.operationDiv}>
        <SimpleAccordion headerTitle="Periodic Tasks" showSearchBox={false}>
          <Box p={2}>
            <CustomButton
              onClick={() => setRunDialogOpen(true)}
              tooltipTitle="Manually trigger a periodic task on the controller"
              enableTooltip={true}
              isDisabled={periodicTaskCount === 0}
            >
              Run Periodic Task
            </CustomButton>
            {periodicTaskCount > 0 && (
              <Box mt={2}>
                <CustomizedTables
                  data={periodicTaskNames}
                  showSearchBox={true}
                  inAccordionFormat={false}
                />
              </Box>
            )}
            {periodicTaskCount === 0 && (
              <Typography variant='body2'>No periodic tasks registered.</Typography>
            )}
          </Box>
        </SimpleAccordion>
      </div>

      {taskTypesTable}

      <CustomDialog
        open={runDialogOpen}
        title="Run Periodic Task"
        handleClose={() => setRunDialogOpen(false)}
        handleSave={handleRunPeriodicTask}
        btnOkText={running ? 'Running...' : 'Run'}
        okBtnDisabled={running || !selectedPeriodicTask}
      >
        <Box display="flex" flexDirection="column" pt={1} pb={1}>
          <FormControl variant="outlined" size="small" className={classes.formField} fullWidth>
            <InputLabel>Periodic Task</InputLabel>
            <Select
              label="Periodic Task"
              value={selectedPeriodicTask}
              onChange={(e) => setSelectedPeriodicTask(e.target.value as string)}
            >
              {periodicTaskNames.records.map((row) => {
                const name = Array.isArray(row) ? row[0] : row;
                return <MenuItem key={String(name)} value={String(name)}>{String(name)}</MenuItem>;
              })}
            </Select>
          </FormControl>
          <Box mt={2}>
            <TextField
              label="Table name (optional, raw or with type suffix)"
              variant="outlined"
              size="small"
              fullWidth
              value={periodicTaskTableName}
              onChange={(e) => setPeriodicTaskTableName(e.target.value)}
            />
          </Box>
          <Box mt={2}>
            <TextField
              label="Table type (optional: OFFLINE / REALTIME)"
              variant="outlined"
              size="small"
              fullWidth
              value={periodicTaskTableType}
              onChange={(e) => setPeriodicTaskTableType(e.target.value)}
            />
          </Box>
          <Box mt={1}>
            <Typography variant='caption'>
              Leaving the table name empty runs the periodic task across all tables.
            </Typography>
          </Box>
        </Box>
      </CustomDialog>
    </Grid>
  );
};

export default MinionTaskManager;
