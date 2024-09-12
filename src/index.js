/********************************************************************************
 * Copyright (C) 2023 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 ********************************************************************************/

// Commercial Licensing Information:
// For commercial use of this software without the copyleft provisions of the AGPLv3,
// you must obtain a commercial license from CoCreate LLC.
// For details, visit <https://cocreate.app/licenses/> or contact us at sales@cocreate.app.

/**
 * CoCreateCronJobs class is responsible for managing cron jobs for organizations.
 * It listens to CRUD events and polls the platform DB to schedule and execute cron jobs.
 * 
 * Example Object:
 * 
 * @typedef {Object} CronJobEvent
 * @property {string} organization_id - The ID of the organization for which the cron job is scheduled.
 * @property {string} nextExecutionTime - The ISO 8601 timestamp for the next time the cron job is scheduled to execute.
 * @property {Object} [schedule] - The scheduling information, including cronExpression, interval, and time-related settings.
 * @property {string} [status] - The current status of the cron job, e.g., 'assigned', 'completed', 'failed'.
 * @property {boolean} [active] - Whether the cron job is active or paused.
 * @property {Object} [job] - The job that is to be executed, which could be $crud, $api, or other specific operations.
 * @property {Object} [retryPolicy] - Retry settings for failed jobs, including retries and backoff strategy.
 * @property {Object} [log] - An optional log of the cron job's last execution details, including status and message.
 *
 * Example Object:
 * 
 * @example
 * {
 *   "organization_id": "652c8d62679eca03e0b116a7",
 *   "nextExecutionTime": "2024-09-10T02:00:00Z",
 *   "active": true,                      // Whether the cron job is active or paused
 *   "status": "assigned",
 *   "schedule": {
 *     "cronExpression": "0 2 * * *",        // Standard cron expression (for Unix-based scheduling)
 *     "startBoundary": "2024-09-10T02:00:00Z",  // The first scheduled execution time
 *     "endBoundary": "2025-09-10T02:00:00Z",    // The end time after which no further executions will occur
 *     "interval": "P1D",                   // ISO 8601 format for repeating interval (e.g., repeat every day)
 *     "daysOfWeek": ["Monday", "Wednesday"], // Days of the week when the job should run
 *     "daysOfMonth": [1, 15],              // Specific days of the month for execution
 *     "months": ["January", "July"],       // Specific months for execution
 *     "skipDates": ["2024-12-25"],         // Dates to skip execution
 *     "timezone": "UTC",                   // Timezone for scheduling
 *     "time": "02:00:00"                  // Specific time of day when the job should run
 *   }, 
 *   "job": {
 *     "$crud": {
 *       "method": "object.update",
 *       "array": "users",
 *       "query": { "stripe.account_id": "acc_123456" },
 *       "object": { "stripe.balance": 1000 }
 *     }
 *   },
 *   "retryPolicy": {
 *     "retries": 3,                      // Number of retries for failed jobs
 *     "retryInterval": "PT10M"           // Interval between retries in ISO 8601 format
 *   },
 *   "backoffStrategy": {
 *     "type": "exponential",             // Type of backoff strategy (exponential, linear, etc.)
 *     "initialDelay": "PT5M"             // Initial delay before the first retry
 *   },
 *   "executionTimeout": "PT2H",           // Timeout for job execution (ISO 8601 format)
 *   "log": {
 *     "timestamp": "2024-09-09T02:00:00Z",
 *     "status": "completed",
 *     "message": "Job executed successfully"
 *   }
 * }
 * 
 * @class CoCreateCronJobs
 */
class CoCreateCronJobs {
    /**
     * Creates an instance of CoCreateCronJobs.
     * @param {Object} crud - The CRUD module used to handle data operations.
     */
    constructor(crud) {
        this.crud = crud;
        this.scheduledJobs = {};

        // Listen for CRUD events related to cron jobs
        process.on('crud-event', (event) => {
            this.handleCrudEvent(event);
        });

        // Start polling every 5 minutes
        setInterval(() => {
            this.pollForCronJobs();
        }, 5 * 60 * 1000);
    }

    /**
     * Handles CRUD events to schedule cron jobs.
     * @param {Object} data - The data object emitted from CRUD operations.
     */
    handleCrudEvent(data) {
        // Check if the array is 'cron-job'
        if (data.array !== 'cron-job') {
            return;  // Ignore non-cron-job events
        }

        // Iterate over the jobs in the data.object array
        for (let i = 0; i < data.object.length; i++) {
            if (data.method === 'object.delete' || data.object[i].active === false) {
                const timeoutId = this.scheduledJobs[data.object[i]._id];

                if (timeoutId) {
                    clearTimeout(timeoutId); // Cancel the timeout if it exists
                    delete this.scheduledJobs[data.object[i]._id]; // Remove the reference
                }

                data.object.splice(i, 1);
                i--;  // Adjust the index after removal

                continue;
            }

            const nextExecutionTime = this.getNextExecutionTime(data.object[i].schedule);

            if (nextExecutionTime) {
                // TODO: Compare executionTime if equal continue
                if (this.scheduledJobs[data.object[i]._id])
                    continue

                const timeUntilExecution = new Date(nextExecutionTime) - new Date();

                data.object[i].nextExecutionTime = nextExecutionTime

                // If the next execution is within the next 5 minutes, schedule the job
                if (timeUntilExecution <= 5 * 60 * 1000 && timeUntilExecution > 0) {
                    data.object[i].status = 'assigned'
                    this.scheduleCronJob(data.object[i]);
                }
            } else {
                data.object.splice(i, 1);
                i--;  // Adjust the index after removal
            }
        }

        this.updateCronJob(data.object);
    }

    /**
     * Parses the schedule object and calculates the next execution time.
     * Takes into account cronExpression, startTime, skipDates, time, daysOfWeek, daysOfMonth, months, endTime, etc.
     * 
     * @param {Object} schedule - The schedule object containing cronExpression, startTime, etc.
     * @returns {string|null} nextExecutionTime - The calculated next execution time in ISO 8601 format, or null if endTime has passed.
     */
    getNextExecutionTime(schedule) {
        const now = new Date();
        let currentDateTime = now;

        // Use startTime if it's greater than now, otherwise use now
        if (schedule.startTime && new Date(schedule.startTime) > now) {
            currentDateTime = new Date(schedule.startTime);
        }

        // Apply skipDates: Skip the date if it matches any in skipDates
        if (schedule.skipDates && schedule.skipDates.includes(currentDateTime.toISOString().split('T')[0])) {
            currentDateTime = new Date(currentDateTime.getTime() + 24 * 60 * 60 * 1000);  // Skip to the next day
        }

        // Apply daysOfWeek: If the current day is not in the allowed days, skip to the next valid day
        if (schedule.daysOfWeek) {
            const dayOfWeek = currentDateTime.toLocaleString('en-US', { weekday: 'long', timeZone: schedule.timezone || 'UTC' });

            while (!schedule.daysOfWeek.includes(dayOfWeek)) {
                currentDateTime.setDate(currentDateTime.getDate() + 1);  // Move to the next day
            }
        }

        // Apply daysOfMonth: Ensure the next execution is on a valid day of the month
        if (schedule.daysOfMonth) {
            while (!schedule.daysOfMonth.includes(currentDateTime.getDate())) {
                currentDateTime.setDate(currentDateTime.getDate() + 1);  // Move to the next day
            }
        }

        // Apply months: Ensure the next execution falls within one of the allowed months
        if (schedule.months) {
            const monthName = currentDateTime.toLocaleString('en-US', { month: 'long', timeZone: schedule.timezone || 'UTC' });

            while (!schedule.months.includes(monthName)) {
                currentDateTime.setMonth(currentDateTime.getMonth() + 1);  // Move to the next month
            }
        }

        // Apply time: Set the execution time of day if specified
        if (schedule.time) {
            const [hours, minutes, seconds] = schedule.time.split(':');
            currentDateTime.setHours(hours, minutes, seconds);
        }

        // Apply endTime: Ensure the next execution time does not exceed the endTime
        if (schedule.endTime && new Date(currentDateTime) > new Date(schedule.endTime)) {
            return null;  // No further executions after endTime
        }

        // Now run the cronParser to handle cronExpression (if present) or fallback to currentDateTime
        if (schedule.cronExpression) {
            const interval = cronParser.parseExpression(schedule.cronExpression, {
                currentDate: currentDateTime,
                tz: schedule.timezone || 'UTC'
            });
            return interval.next().toISOString();
        }

        return currentDateTime.toISOString();
    }

    /**
     * Schedules a cron job to be executed at a specific datetime.
     * 
     * @param {string} organization_id - The organization ID.
     * @param {string} datetime - The time at which the job should be executed.
     */
    scheduleCronJob(object) {
        const delay = new Date(object.nextExecutionTime) - new Date();

        if (delay <= 0) {
            this.executeCronJob(object);
            return;
        }

        console.log(`Scheduling cron job for org ${object.organization_id} in ${delay / 1000} seconds`);

        // TODO: get clusterId, serverId and workerId
        object = {
            _id: object._id,
            nextExecutionTime: object.nextExecutionTime,
            status: object.status,
            clusterId: 'id',
            severId: 'id',
            workerId: 'id',
            organization_id: object.organization_id
        }

        this.scheduledJobs[object._id] = setTimeout(() => {
            this.executeCronJob(object);
        }, delay);
    }

    /**
     * Executes the scheduled cron job.
     * 
     * @param {Object} schedule - The cron-job object containing job, schedule, etc.
     */
    async executeCronJob(object) {
        console.log(`Executing cron job for org ${object.organization_id}`);

        if (!object.job) {
            let data = await this.crud.send({
                method: 'object.read',
                array: 'cron-job',
                object: { _id: object._id },
                organization_id: object.organization_id
            })
            object = data.object[0]
            console.log('Fetched cron job data:', object);
        }

        // TODO: execute cron using lazyloader.webhook()
        object.nextExecutionTime = this.getNextExecutionTime(object.schedule);
        if (!object.nextExecutionTime) {
            object.active = false
            object.status = 'completed'
        }

        this.updateCronJob(object);
    }


    /**
     * Polls the platform DB for cron jobs that are scheduled or potentially failed.
     */
    pollForCronJobs() {
        console.log('Polling for cron jobs...');

        const now = new Date();
        const fiveMinutesFromNow = new Date(now.getTime() + 5 * 60 * 1000);
        const oneMinuteAgo = new Date(now.getTime() - 1 * 60 * 1000);  // 1 minute past due jobs

        this.crud.send({
            method: 'object.read',
            array: 'organizations',
            filter: {
                query: {
                    $or: [
                        {
                            $and: [
                                { 'cron-jobs.nextExecutionTime': { $elemMatch: { $lte: fiveMinutesFromNow, $gte: now } } }, // Match jobs in array within 5 minutes
                                { 'cron-jobs.status': { $elemMatch: { $ne: 'assigned' } } },   // Not already assigned
                                { 'cron-jobs.active': { $elemMatch: { $ne: false } } }   // Is active
                            ]
                        },
                        {
                            $and: [
                                { 'cron-jobs.nextExecutionTime': { $elemMatch: { $lt: oneMinuteAgo } } },   // Past due by 1 minute
                                { 'cron-jobs.status': { $elemMatch: { $eq: 'assigned' } } },    // Assigned but not started
                                { 'cron-jobs.active': { $elemMatch: { $ne: false } } }   // Is active                              
                            ]
                        }
                    ]
                }
            },
            organization_id: process.env.organization_id
        }).then(data => {
            for (let i = 0; i < data.object.length; i++) {
                const cronJobs = data.object[i].cronJobs;
                if (!cronJobs)
                    return

                // Check each cron job in the organization's `cron-jobs` array
                for (let j = 0; j < cronJobs; j++) {
                    if (this.scheduledJobs[cronJobs[j]._id])
                        return

                    cronJobs[j].status = 'assigned'

                    // TODO: get clusterId, serverId and workerId
                    cronJobs[j].clusterId = 'id'
                    cronJobs[j].severId = 'id'
                    cronJobs[j].workerId = 'id'
                    cronJobs[j].organization_id = data.object[i].organization_id

                    this.scheduleCronJob(cronJobs[j]);
                }

                this.updateCronJob(cronJobs);
            }
        }).catch(error => {
            console.error('Error polling for cron jobs:', error);
        });
    }

    /**
     * Marks the cron job as failed after it is detected as past due and not started.
     */
    updateCronJob(object) {
        if (!Array.isArray(object))
            object = [object]

        for (let i = 0; i < object.length; i++) {
            this.crud.send({
                method: 'object.update',
                array: 'cron-jobs',
                object,
                upsert: true,
                organization_id: object.organization_id
            }).then(() => {
                console.log(`Cron job for org ${object.organization_id} updated.`);
            }).catch(error => {
                console.error(`Error updating cron job for org ${object.organization_id}:`, error);
            });

        }

        const cronJobs = object
        this.crud.send({
            method: 'object.update',
            array: 'organizations',
            object: {
                _id: object.organization_id,
                cronJobs
            },
            organization_id: process.env.organization_id
        }).then(() => {
            console.log(`Cron job for org ${object.organization_id} updated.`);
        }).catch(error => {
            console.error(`Error updating cron job for org ${object.organization_id}:`, error);
        });
    }

    /**
     * Adds a cron job reference to the platform database.
     * 
     * @param {string} organization_id - The organization ID.
     * @param {string} datetime - The datetime of the cron job to add to the platform DB.
     */
    addToPlatformDB(organization_id, datetime) {
        console.log(`Adding cron job reference to platform DB for org ${organization_id}`);

        this.crud.send({
            method: 'object.read',
            array: 'organizations',
            object: { _id: organization_id, nextCron: datetime },
            organization_id: process.env.organization_id
        }).then(() => {
            console.log(`Platform DB updated for org ${organization_id}`);
        }).catch(error => {
            console.error(`Error updating platform DB for org ${organization_id}:`, error);
        });
    }

    /**
    * Finds and schedules the next cron job for the organization after one is executed.
    * 
    * @param {string} organization_id - The organization ID.
    */
    findNextCronJob(organization_id) {
        console.log(`Finding next cron job for org ${organization_id}`);

        this.crud.read({
            collection: 'cron-job',
            query: { organization_id },
            sort: { datetime: 1 },
            limit: 1
        }).then(nextJob => {
            if (nextJob.length > 0) {
                const { datetime } = nextJob[0];
                this.scheduleCronJob(organization_id, datetime);
            }
        }).catch(error => {
            console.error(`Error finding next cron job for org ${organization_id}:`, error);
        });
    }
}

module.exports = CoCreateCronJobs;
