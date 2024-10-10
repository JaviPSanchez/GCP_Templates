# GCP Native Project Pipeline

![Image](../assets/gcp-native.png "GCP Professional Engineer Projects")

Welcome to the **GCP Native Project Templates** repository! This project demonstrates how to set up a fully native GCP pipeline using various Google Cloud services to build a robust, scalable data platform. The project leverages a combination of GCP services to integrate data, process it, and visualize insights effectively.

In this project, we implement two different types of pipelines: OLTP (Online Transaction Processing) and OLAP (Online Analytical Processing). Each serves a specific purpose in managing and processing data within the system

## Services Used

1. **Cloud Scheduler**  
   Trigger and manage scheduled jobs for automation.

2. **Cloud Pub/Sub**  
   Message queue for real-time data integration.

3. **Cloud Functions**  
   Event-driven serverless compute for running microservices.

4. **Cloud SQL**  
   Managed relational database service using MySQL.

5. **Looker Studio**  
   Data visualization and reporting for creating real-time dashboards.

6. **Cloud Storage**  
   Scalable object storage for data files and assets.

7. **BigQuery**  
   Fully managed data warehouse for analytics.

8. **Grafana**  
   Monitoring and observability dashboard for data visualization.

9. **VPC Network**  
   Isolated network infrastructure for managing resources securely.

10. **IAM & Admin**  
    Identity and access management for securing and managing permissions.

11. **Logging**  
    Centralized logging using Google Cloud Logging for monitoring application performance.

## Project Overview

This project focuses on building a data pipeline using **Cloud Functions**, **Cloud SQL**, and other GCP services. We aim to connect and orchestrate services to ingest, store, and visualize data using modern, serverless technologies.

### Key Features

- **Automated Scheduling**: Use Cloud Scheduler to automate data ingestion and processing.
- **Real-time Processing**: Cloud Pub/Sub and Cloud Functions enable real-time data processing workflows.
- **Scalable Database**: Cloud SQL serves as the core relational database for data storage.
- **Data Visualization**: Visualize results with Looker Studio and Grafana.
- **Logging and Monitoring**: Implement robust logging and monitoring practices with Cloud Logging and Grafana.

## Connecting Cloud SQL to Local Environment

After setting up **Cloud SQL** (MySQL) on a **Compute Engine**, you may want to connect from your local environment (e.g., Visual Studio Code). Follow the steps below to configure the required permissions.

### Steps to Set Up Permissions

#### 1. Enable Cloud SQL Admin API

Make sure the **Cloud SQL Admin API** is enabled for your project. You can enable it by:

1. Navigating to the Google Cloud Console API Library.
2. Searching for "Cloud SQL Admin API."
3. Clicking "Enable" on the API page.

#### 2. Grant Cloud SQL Client Role

Ensure your service account has the appropriate permissions to connect to Cloud SQL.

1. Go to the **IAM & Admin Console**.
2. Find the service account associated with your `key_access_sql.json`.
3. Click the pencil icon to edit the roles for this service account.
4. Add the **Cloud SQL Client** role (`roles/cloudsql.client`).

> This role allows the service account to connect to Cloud SQL instances.

#### 3. (Optional) Additional IAM Roles

For administrative tasks, you might need to add the following roles:

- **Cloud SQL Admin** (`roles/cloudsql.admin`): Full management access to Cloud SQL resources.
- **Cloud SQL Viewer** (`roles/cloudsql.viewer`): Read-only access to Cloud SQL resources.

#### 4. Verify Service Account Key

Ensure the service account JSON key you're using (`key_access_sql.json`) has the correct permissions. Verify the roles of this service account by reviewing its permissions in the **IAM & Admin Console**.

#### 5. Check GCP Billing

Make sure your GCP project is linked to an active billing account to avoid any restrictions on API usage.

---

## How to Run

1. Clone this repository:
   ```bash
   git clone https://github.com/JaviPSanchez/GCP_Templates.git
   ```
2. Navigate to the specific project directory:

   ```bash
   cd GCP_Templates/01-GCP-Native
   ```

3. Follow the steps outlined in the documentation to set up the GCP services for this project.

---

## Future Enhancements

- Integration with **Cloud Run** for containerized workloads.
- Expansion of the pipeline with **Airflow** for complex orchestration.
- Application of **BigQuery ML** for machine learning predictions directly in the data warehouse.

---

Feel free to contribute to this project by opening issues or submitting pull requests. Let's build scalable, serverless data solutions on GCP together!
