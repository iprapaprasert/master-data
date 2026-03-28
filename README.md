# In caution
- Please do not use `ses_ef_dim_expenditure` and `ses_eg_dim_expenditure`. These files are in error, still developing.
- `ses_conv_expenditure_io` is still developing, but you can use it. However, watch for frequently update.

# Flattening
Most dimension tables are in child-parent hierarchy format (hierarchical format) (as it is recommened by Microsoft). SQL Server can handle them easily by declaring as `HIERARCHYID` or in Power BI they have a group of parent-child hierarchy function. However, for some use cases that need to flatten them first. The python's PySpark code `flatten_hierarchy.py` can do this job, or you can use CTE expression in SQL software to do this.

## Notes on convention of flattening
| File | Maxmimum Level (`maxlvl`) | Recommended Name (from the lowest-level to the highest-level) |
| --- | --- | --- |
| `nso_dim_industry.csv` | 5 | `IndustryKey` -> `IndustryClassKey` -> `IndustryGroupKey` -> `IndustryDivisionKey` -> `IndustrySectionKey` |

# Notes and clarification
## Correspondence table of TSIC to IO Table Industry (`nesdc_conv_tsic_io`)
Only 7 sectors cannot be mapped from TSIC to IO
- `01500` Mixed farming
- `01619`
- `09100`
- `38120`
- `43309`
- `77299`
