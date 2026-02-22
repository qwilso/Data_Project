SUMMARY
In this project, I bridged the gap between raw, disconnected data sources and the Finance team’s reporting needs. By designing an automated ETL (Extract, Transform, Load) process, I reduced manual data entry by 5 hours per week and eliminated errors caused by inconsistent formatting across regional offices.

Business Challenge
The Finance team was receiving sales reports in different formats (CSV, Excel, and API exports). Because the data wasn't "talking to each other", leadership couldn't get an accurate view of weekly revenue without hours of manual cleanup.
MY GOAL: Create a unified "Single Source of Truth" that prepares this data for analysis automatically.

THE SOLUTION
I will be both the Architect and Builder for this integration:
Requirements Gathering (BA Role): Interviewed stakeholders to define "Clean Data" and identified key performance indicators (KPIs) needed for their dashboards.
Logic Design: Mapped out how data should flow from the source to the final report, ensuring that sensitive information was masked and currency formats were standardized.
ETL Development: Built a pipeline using Informatica Cloud Data Integration to:
  -Extract: Pull data automatically from various department folders.
  -Transform: Cleanse the data (removing duplicates, fixing date formats, and calculating tax totals).
  -Load: Deliver a "Ready-to-Analyze" dataset for the Data Analysis team.

BUSINESS IMPACT
Efficiency: Automated a process that previously took 4 hours of manual labor every Monday morning.
Accuracy: Improved data reliability by implementing automated validation checks (e.g., ensuring no negative sales amounts were processed).
Scalability: The logic I built is modular, meaning new regional offices can be added to the flow in minutes rather than days.
