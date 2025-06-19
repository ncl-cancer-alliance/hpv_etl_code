--DROP TABLE [Data_Lab_NCL_Dev].[GrahamR].[Hpv_Data]

CREATE TABLE [Data_Lab_NCL_Dev].[GrahamR].[Hpv_Data](
	Local_Authority varchar(40),
	Year_Group varchar(10),
	Gender varchar(20),
	Number int,
	Number_Vaccinated int,
	Academic_Year_End_Date date,
	Academic_Year_Text varchar(40),
	Extract_Date date
) 
