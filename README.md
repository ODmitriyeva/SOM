# SOM 
4/22/2019 : system optimization
Calulations of Margin, OPEX all the way to EBITDA including additional transformation for UI
DATA pull from Snowflake SOM
performance optimization done by Chris Snyder 4/18/2019




4/23/2019
OPEX is now calculated over the whole DCP once a month. Margin can be calculated by area. prod4-DJBasin is a version of 6 months Margin calcs for DJ Basin area 


4/26/2018
Margin+OPEX+UI calcs for a given month done throughout the whole meter_coefficient table (performance: about 8 hours on a single CPU on Azure machine). File Margin-OPEX-UI_Jan2019.py

8/10/2019 
Performance_deconstructed - jupyter notebook for monthly deconstructed view calculations done route by route performance boosted.
