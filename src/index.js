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

class CoCreateCronJobs {
    constructor(crud) {
        this.crud = crud; // CRUD module passed in
        this.scheduledJobs = {}; // To keep track of scheduled jobs

        // Listen to the 'crud-event'
        this.crud.on('crud-event', (event) => {
            this.handleCrudEvent(event);
        });

        // Start polling every 5 minutes
        setInterval(() => {
            this.pollForCronJobs();
        }, 5 * 60 * 1000); // Poll every 5 minutes
    }

    // Function to handle the 'crud-event'
    handleCrudEvent(event) {
        const { collection, data } = event;

        // Check if the event relates to a cron job collection
        if (collection === 'cron-job') {
            const { organization_id, datetime } = data;

            // Check if the cron job needs to be scheduled within the next 5 minutes
            const timeUntilExecution = new Date(datetime) - new Date();

            if (timeUntilExecution <= 5 * 60 * 1000 && timeUntilExecution > 0) {
                // Schedule the cron job immediately if within the next 5 minutes
                this.scheduleCronJob(organization_id, datetime);
            } else {
                // Otherwise, add a reference to the platform DB to be picked up by polling
                this.addToPlatformDB(organization_id, datetime);
            }
        }
    }

    // Function to schedule the cron job
    scheduleCronJob(organization_id, datetime) {
        // Calculate delay until the job needs to be executed
        const delay = new Date(datetime) - new Date();

        if (delay <= 0) {
            console.log(`Skipping past-due job for org ${organization_id}`);
            return;
        }

        console.log(`Scheduling cron job for org ${organization_id} in ${delay / 1000} seconds`);

        // Set a timeout to execute the cron job
        this.scheduledJobs[organization_id] = setTimeout(() => {
            this.executeCronJob(organization_id);
        }, delay);
    }

    // Function to execute the cron job
    executeCronJob(organization_id) {
        console.log(`Executing cron job for org ${organization_id}`);

        // Fetch the actual cron document from the organization's DB using the CRUD module
        this.crud.read({
            collection: 'cron-job',
            query: { organization_id },
        }).then(cronJobData => {
            // Process and execute the cron job based on the fetched data
            console.log('Fetched cron job data:', cronJobData);

            // After executing, find the next cron job for the organization
            this.findNextCronJob(organization_id);
        }).catch(error => {
            console.error(`Error fetching cron job for org ${organization_id}:`, error);
        });
    }

    // Function to add cron job reference to platform DB for polling
    addToPlatformDB(organization_id, datetime) {
        console.log(`Adding cron job reference to platform DB for org ${organization_id}`);

        this.crud.update({
            collection: 'organizations',
            query: { _id: organization_id },
            update: { nextCronExecutionTime: datetime },
        }).then(() => {
            console.log(`Platform DB updated for org ${organization_id}`);
        }).catch(error => {
            console.error(`Error updating platform DB for org ${organization_id}:`, error);
        });
    }

    // Poll for cron jobs that need to be executed soon (within 5 minutes)
    pollForCronJobs() {
        console.log('Polling for cron jobs...');

        // Query the platform DB for organizations with a nextCronExecutionTime within 5 minutes
        const now = new Date();
        const fiveMinutesFromNow = new Date(now.getTime() + 5 * 60 * 1000);

        this.crud.read({
            collection: 'organizations',
            query: {
                nextCronExecutionTime: { $lte: fiveMinutesFromNow },
            },
        }).then(organizations => {
            organizations.forEach(org => {
                const { _id: organization_id, nextCronExecutionTime } = org;

                // Schedule the cron job if it hasn’t already been scheduled
                if (!this.scheduledJobs[organization_id]) {
                    this.scheduleCronJob(organization_id, nextCronExecutionTime);
                }
            });
        }).catch(error => {
            console.error('Error polling for cron jobs:', error);
        });
    }

    // Find and schedule the next cron job for the organization after one is executed
    findNextCronJob(organization_id) {
        console.log(`Finding next cron job for org ${organization_id}`);

        this.crud.read({
            collection: 'cron-job',
            query: { organization_id },
            sort: { datetime: 1 }, // Sort by the next closest datetime
            limit: 1,
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
