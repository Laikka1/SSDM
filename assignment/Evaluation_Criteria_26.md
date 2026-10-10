# **CS-E4780 Scalable Systems and Data Management** 

Course Project Evaluation Criteria (2026) _Detecting Trading Trends in Financial Tick Data_ 

The course project is graded out of **26 points** : system implementation (12), final report (12), and teamwork and individual contributions (2). Each criterion in the implementation and report tables is scored on a four-level scale from Unacceptable (0) to Exceptional (3). The criteria refer to the queries and requirements defined in the project description: Query 1 (exponential moving averages over 5-minute tumbling windows), Query 2 (breakout detection producing buy/sell advisories), and the optional smart visualization. The total maps to course grades as shown on the right. 

|**Grade**|**Points**|
|---|---|
|5|24 – 26|
|4|19 – 23|
|3|15 – 18|
|2|11 – 14|
|1|1 – 10|
|0|0|



# **System implementation (12 points)** 
s
|**Criterion**|**Unacceptable (0)**|**Marginal (1)**|**Acceptable (2)**|**Exceptional (3)**|
|---|---|---|---|---|
|**Functionality**|Unable to correctly<br>ingest or process the<br>real tick data;<br>neither query<br>produces results.|Basic functionality:<br>the data is ingested<br>and EMAs<br>(Query 1) are<br>computed, but<br>results contain<br>frequent errors, or<br>crossover detection<br>(Query 2) is missing<br>or unreliable.|Both queries work<br>as intended with<br>minor bugs, e.g.<br>occasional incorrect<br>EMA values, or<br>missed or spurious<br>buy/sell advisories.|Full functionality:<br>correct EMA38 and<br>EMA100 per window<br>and correct buy/sell<br>advisories for all<br>subscribed symbols,<br>with no significant<br>issues.|
|**Performance**|System is unable to<br>process even small<br>portions of the<br>dataset without<br>significant lag.|Can process part of<br>the dataset but<br>struggles with larger<br>portions or with<br>real-time processing<br>requirements.|Processes large<br>portions of the<br>dataset efficiently,<br>with occasional<br>performance dips.|Processes the entire<br>week of data<br>smoothly, sustaining<br>high throughput<br>with low and stable<br>latency.|
|**Scalability**|Cannot scale to<br>handle multiple<br>exchanges or high<br>event volumes.|Limited scalability;<br>handles data from a<br>single exchange or a<br>reduced dataset<br>only.|Scales to handle<br>data across all three<br>exchanges with<br>moderate event<br>volumes.|Scales effectively<br>across all exchanges<br>and event volumes,<br>including peak rates<br>and the long-tail<br>symbol distribution,<br>with capacity for<br>further increases<br>(more symbols,<br>more subscribers,<br>more resources).|
|**Creative**<br>**explorations**|No exploration of<br>the design space; a<br>single approach with<br>no justification.|Some alternatives<br>are mentioned (e.g.<br>windowing or state<br>management<br>choices), but<br>trade-offs are not<br>analysed.|Balanced trade-offs<br>in the design space<br>and system<br>architecture are<br>discussed and<br>justified, e.g.<br>partitioning by<br>symbol, state<br>management, or<br>handling of late and<br>out-of-order events.|Well-reasoned<br>exploration of the<br>design space<br>supported by<br>measurements,<br>and/or a smart<br>visualization that<br>makes the query<br>results directly<br>interpretable<br>(Section 3.4 of the<br>project description).|



1 

# **Final report (12 points)** 

The report follows the ACM proceedings template (L<sup>A</sup> TEX or Word), is 4–5 pages long, and is submitted together with a link to the system’s GitHub repository. 

|**Criterion**|**Unacceptable (0)**|**Marginal (1)**|**Acceptable (2)**|**Exceptional (3)**|
|---|---|---|---|---|
|**System design**<br>**and architecture**|No clear description<br>of the system design.|Basic design, but<br>lacks support for<br>real-time or complex<br>event processing<br>needs.|Includes design<br>elements for<br>real-time data<br>processing,<br>metadata handling,<br>and normalization.|Well-architected<br>system design that<br>handles complex<br>event processing,<br>real-time<br>requirements, and<br>scalability.|
|**Implementation**<br>**explanation**|Explanation of the<br>implementation is<br>missing or very<br>unclear.|Limited details<br>provided, with<br>minimal explanation<br>of the handling of<br>tick and<br>housekeeping events.|Clear explanation of<br>how tick,<br>housekeeping, and<br>metadata events are<br>processed, including<br>the hardware or<br>cloud services and<br>programming tools<br>used.|Detailed explanation<br>covering all<br>implementation<br>aspects, including<br>event handling,<br>windowing and<br>EMA state,<br>crossover detection,<br>normalization, and<br>metadata usage.|
|**Evaluation and**<br>**performance**|No meaningful<br>evaluation or<br>performance testing<br>presented.|Basic evaluation,<br>with limited testing<br>on data handling for<br>events and<br>metadata.|Performance testing<br>covers correctness of<br>the query results<br>and handling of<br>different event types<br>and distribution<br>patterns.|Comprehensive<br>evaluation:<br>correctness,<br>throughput and<br>latency under load,<br>scalability across<br>exchanges and with<br>varying resources<br>(CPU/GPU<br>utilization), and<br>handling of long-tail<br>distributions.|
|**Academic writing**|Report is poorly<br>structured and lacks<br>coherence.|Basic structure, but<br>lacks clarity in<br>discussing data<br>handling and event<br>distribution.|Well-structured and<br>clear, with minor<br>issues in<br>presentation or<br>analysis. Citation<br>format is consistent.|Professionally<br>written, clear, and<br>logically structured,<br>with a strong focus<br>on dataset analysis.<br>Follows the template<br>and page limit.|



# **Teamwork and individual contributions (2 points)** 

Each team member must answer the following questions as _individual contributions_ in the report: 

1. What are the most important decisions you and your group have made about the system design? 

2. What are the most challenging parts during the design and implementation? 

3. What is your specific contribution in the team? How do you characterize the team’s overall functionality? 

|**Criterion**|**Unacceptable (0)**|**Acceptable (1)**|
|---|---|---|
|**Individual**<br>**contributions**|No meaningful individual contributions<br>documented.|Clear description of contributions and<br>reflection.|
|**Teamwork**|Team collaboration is ineffective, and roles<br>are unclear.|Exemplary teamwork, with clear roles,<br>efficient data management, and active<br>collaboration.|



Students working alone are evaluated as a one-member team using the same criteria, except for the teamwork part. 

2 

