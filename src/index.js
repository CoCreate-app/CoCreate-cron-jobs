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
 * @property {string} datetime - The ISO 8601 timestamp when the cron job is scheduled to execute.
 * @property {Object} [task] - The task that is to be executed. This could be $crud, $api, or other specific operations.
 * @property {boolean} [scheduled] - Whether the cron job has already been scheduled for execution.
 * @property {string} [nextCronExecutionTime] - The next time the cron job will execute, typically used in polling.
 * @property {Object} [retryPolicy] - Retry settings for failed jobs, including retries and backoff strategy.
 * @property {Object} [log] - An optional log of the cron job's last execution details.
 *
 * Example Object:
 * 
 * @example
 * {
 *   "organization_id": "652c8d62679eca03e0b116a7",
 *   "datetime": "2024-09-10T02:00:00Z",
 *   "task": {
 *     "$crud": {
 *       "method": "object.update",
 *       "collection": "users",
 *       "query": { "stripe.account_id": "acc_123456" },
 *       "object": { "stripe.balance": 1000 }
 *     }
 *   },
 *   "scheduled": false,
 *   "nextCronExecutionTime": "2024-09-10T02:00:00Z",
 *   "retryPolicy": {
 *     "retries": 3,
 *     "retryInterval": "PT10M"
 *   },
 *   "log": {
 *     "timestamp": "2024-09-09T02:00:00Z",
 *     "status": "completed",
 *     "message": "Task executed successfully"
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
        this.crud.on('crud-event', (event) => {
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
        // Check if the collection is 'cron-job'
        if (data.collection !== 'cron-job') {
            return;  // Ignore non-cron-job events
        }

        // Iterate over the tasks in the data.object array
        for (let task of data.object) {
            const nextExecutionTime = this.getNextExecutionTime(task.schedule);

            if (nextExecutionTime) {
                const timeUntilExecution = new Date(nextExecutionTime) - new Date();

                // If the next execution is within the next 5 minutes, schedule the job
                if (timeUntilExecution <= 5 * 60 * 1000 && timeUntilExecution > 0) {
                    this.scheduleCronJob(task.organization_id, nextExecutionTime);
                } else {
                    // Otherwise, add to platform DB for polling
                    this.addToPlatformDB(task.organization_id, nextExecutionTime);
                }
            }
        }
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
    scheduleCronJob(organization_id, datetime) {
        const delay = new Date(datetime) - new Date();

        if (delay <= 0) {
            console.log(`Skipping past-due job for org ${organization_id}`);
            return;
        }

        console.log(`Scheduling cron job for org ${organization_id} in ${delay / 1000} seconds`);

        this.scheduledJobs[organization_id] = setTimeout(() => {
            this.executeCronJob(organization_id);
        }, delay);
    }

    /**
     * Executes the scheduled cron job.
     * 
     * @param {string} organization_id - The organization ID.
     */
    executeCronJob(organization_id) {
        console.log(`Executing cron job for org ${organization_id}`);

        this.crud.read({
            collection: 'cron-job',
            query: { organization_id }
        }).then(cronJobData => {
            console.log('Fetched cron job data:', cronJobData);
            this.findNextCronJob(organization_id);
        }).catch(error => {
            console.error(`Error fetching cron job for org ${organization_id}:`, error);
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

        this.crud.update({
            collection: 'organizations',
            query: { _id: organization_id },
            update: { nextCronExecutionTime: datetime }
        }).then(() => {
            console.log(`Platform DB updated for org ${organization_id}`);
        }).catch(error => {
            console.error(`Error updating platform DB for org ${organization_id}:`, error);
        });
    }

    /**
     * Polls the platform DB for cron jobs scheduled within the next 5 minutes.
     */
    pollForCronJobs() {
        console.log('Polling for cron jobs...');

        const now = new Date();
        const fiveMinutesFromNow = new Date(now.getTime() + 5 * 60 * 1000);

        this.crud.read({
            method: 'object.read',
            collection: 'organizations',
            filter: {
                query: {
                    $and: [
                        { 'cron-jobs.datetime': { $lte: fiveMinutesFromNow } },
                        { 'cron-jobs.datetime': { $gte: now } },
                        {
                            $or: [
                                { 'cron-jobs.scheduled': false },
                                { 'cron-jobs.scheduled': { $exists: false } },
                                { 'cron-jobs.scheduled': null },
                                { 'cron-jobs.datetime': { $lt: now }, 'cron-jobs.scheduled': true }
                            ]
                        }
                    ]
                }
            }
        }).then(organizations => {
            organizations.forEach(org => {
                const { _id: organization_id, 'cron-jobs': { datetime } } = org;

                if (!this.scheduledJobs[organization_id]) {
                    this.scheduleCronJob(organization_id, datetime);
                    this.markCronJobAsScheduled(organization_id);
                }
            });
        }).catch(error => {
            console.error('Error polling for cron jobs:', error);
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
