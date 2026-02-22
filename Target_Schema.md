Target Field Name	Data Type	Source Logic / Transformation	Business Definition
TRANS_ID	Integer	Order_ID	Unique identifier for the transaction.
CUSTOMER_FULL_NAME	String	INITCAP(First_Name || ' ' || Last_Name)	Combined and standardized name for reporting.
CALENDAR_DATE	Date	TO_DATE(Trans_Date, 'MM/DD/YYYY')	Standardized date for year-over-year analysis.
REPORTING_REGION	String	UPPER(Region)	Cleaned regional tag for filtering dashboards.
NET_SALES_AMT	Decimal	TO_DECIMAL(REG_REPLACE(Raw_Amount, '[$,]', ''))	Clean numeric value (removed "$" and commas).
TAX_AMT	Decimal	Net_Sales_Amt * Tax_Rate	Calculated Field: Total tax for the transaction.
GROSS_SALES_AMT	Decimal	Net_Sales_Amt + Tax_Amt	Calculated Field: Total revenue including tax.
LOAD_TIMESTAMP	Timestamp	SESSSTARTTIME	Audit column showing when this data was processed.