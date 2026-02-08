# PopHIVE Canada: Same great data, more maple flavor

## Using these data

The data shown on PopHIVE.org are found in the Ingest project project in the ./Data/bundle_*/dist/ subfolders. The files are stored in either parquet or compressed csv format. If using R, parquet files can be downloaded using the arrow package in R. For example:

library(arrow)

url1 <- 'https://github.com/PopHIVE/Ingest/raw/refs/heads/main/data/bundle_respiratory/dist/covid_overall_trends.parquet'

ds1 <- read_parquet(url1)

compressed csv can be downloaded with vroom::vroom() in R:

url2 <- 'https://github.com/PopHIVE/Ingest/raw/refs/heads/main/data/nchs_mortality/standard/data_county.csv.gz'

ds2 <- vroom::vroom(url2)

In general, the data closest to the source data are found in the 'value' column. Some datasets also include a 3 week moving average (value_smooth), and a smoothed value, scaled to between 0-100 (value_smooth_scale). The data in 'value' are generally drawn directly from the source data. Exceptions include:

1)  In some datasets where national level data were not provided by the source, a national average is calculated using a population-weighted average.

2)  For Epic Cosmos, if the data are based on fewer than 10 counts, the cell is suppressed. For visualization purposes, this is filled in with a value halfway between 0 and the minimum value reported for that state. These values are indicated with suppressed_flag=1.

Time-stamped archives of the data are available in the Pulled Data folder.

## FAQ

*Can I re-use the data from PopHIVE?*

Yes! Much of the data are drawn from publicly available. Other data, including the results of research performed using Epic Cosmos or the data available through Google Health Trends, can be used with appropriate attribution. A suggested citation relating to this data is 'Results of research performed with Epic Cosmos were obtained from the PopHIVE platform [url for Github corresponding to the specific data source].’

Please cite the use of data from PopHIVE and the original source. the DOI for PopHIVE Canada is TBD.


*Who is it for?* PopHIVE is designed for a broad audience: - Members of the public who want to understand what’s happening in their communities. - Clinicians who need to anticipate trends and adjust care. - Public health departments and local governments who need up-to-date data to allocate resources. - Researchers, journalists, and advocates working to tell stories and drive policy change. - Policy makers and decision-makers who need to understand the basics of who, what, and where about health issues occurring in the areas they serve.

*Will you show additional conditions in the future?* Yes. PopHIVE is evolving based on user needs and feedback. As high-quality, de-identified data becomes available, we plan to expand condition-specific dashboards, such as those for diabetes, maternal health, and behavioral health. Please provide us feedback on what you’d like to see here.

*How do I know the data is accurate or reliable?* PopHIVE’s data team continually evaluates the quality and representativeness of the data. In some cases (like diabetes Hemoglobin A1C data), completeness varies, and we are committed to transparency about what the data can and can’t tell us. This is an evolving platform, and we're building new functionality and insights over time.

*How are you using electronic health record data from Epic? Isn’t that a violation of HIPAA?* PopHIVE doesn’t change any rules or regulations around health data sharing. We only use de-identified, aggregate data, following all existing privacy laws. We’re not sharing individual patient records—we’re simply making existing public health trends more timely and accessible for the public good.

*Are you accepting additional data sets?* Yes! We welcome partnerships and are actively working to expand PopHIVE’s data offerings. If you have a reliable, de-identified dataset that could help improve public understanding of health, we’d love to hear from you. Please submit here.

*How can I give feedback on this tool?* We’d love to hear from you. PopHIVE is shaped by the people who use it. Whether you have a technical suggestion, want to request a feature, or share how it helped your community, please submit [here](https://docs.google.com/forms/d/e/1FAIpQLSchAasiq7ovCCNz9ussb7C2ntkZ-8Rjc7-tNSglkf5boS-A0w/viewform?pli=1).



# Guide to adding data and rebuilding the bundle



# Legal Disclaimer

These data and PopHIVE statistical outputs are provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors, contributors, or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the data or the use or other dealings in the data.

The PopHIVE statistical outputs are research tools intended for use in the fields of public health and medicine. They are not intended for clinical decision making, are not intended to be used in the diagnosis or treatment of patients and may not be useful or appropriate for any clinical purpose. Users of the PopHIVE statistical outputs should be aware of their responsibilities to ensure the ethical and appropriate use of this technology, including adherence to any applicable legal and regulatory requirements.

The content and data provided with the statistical outputs do not replace the expertise of healthcare professionals. Healthcare professionals should use their professional judgment in evaluating the outputs of the PopHIVE statistical outputs.
