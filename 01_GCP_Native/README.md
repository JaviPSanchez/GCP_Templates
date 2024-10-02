### First Project

![Image](./assets/gcp-native.png "GCP Professional Engineer Projects")

1.  Cloud Scheduler
2.  Cloud Pub/Sub
3.  Cloud Functions
4.  Cloud SQL
5.  Looker Studio
6.  Cloud Storage
7.  BigQuery
8.  Grafana
9.  VPC Network
10. IAM & Admin

After the creation of the Compute Engine with Mysql we want to connect directly from our local
environment, in this case Visual Studio Code. We will need permissions

### Steps to set the Permissions

Enable Cloud SQL Admin API:

Make sure that the Cloud SQL Admin API is enabled for your GCP project.

To enable it:

Go to the Google Cloud Console API Library.
Search for Cloud SQL Admin API.
Click on the API and click Enable.
Grant the Cloud SQL Client Role:

You need to ensure that the service account you’re using has the correct IAM roles to access Cloud SQL.

To grant the role:

Go to the IAM & Admin Console.

Find the service account associated with your key_access_sql.json.

Click the pencil icon to edit roles for this service account.

Add the Cloud SQL Client role (roles/cloudsql.client).

This role allows the service account to connect to Cloud SQL instances.

Additional IAM Roles (Optional):

If you're performing administrative tasks on Cloud SQL, you might also need:

Cloud SQL Admin (roles/cloudsql.admin): Grants full management access to Cloud SQL resources.
Cloud SQL Viewer (roles/cloudsql.viewer): Read-only access to Cloud SQL resources.
Verify Permissions in Service Account Key:

Ensure that the service account JSON key you're using (key_access_sql.json) is the correct one and has the required permissions. You can verify the roles of the service account by checking its permissions in the IAM & Admin Console.

Check GCP Billing: Make sure your GCP project is linked to an active billing account. Sometimes, restricted API usage occurs due to billing-related issues.
