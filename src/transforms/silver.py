############################# UK CPI #############################
# uk_cpi_monthly_df = full_cpi_df.filter(col('period').rlike(r"^\d{4} [A-Z]{3}$"))
# uk_cpi_quarterly_df = full_cpi_df.filter(col('period').rlike(r"^\d{4} Q[1-4]$"))
# uk_cpi_yearly_df = full_cpi_df.filter(~(col("period").rlike(r"^\d{4} [A-Z]{3}$") | col("period").rlike(r"^\d{4} Q[1-4]$")))