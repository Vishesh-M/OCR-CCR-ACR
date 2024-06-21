rm(list=ls())

#===================
# 0.0 Libraries ####
#===================

options(stringsAsFactors=F)
library(salesforcer)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(tidylog)
library(lubridate)

#====================
# 1.0 Parameters ####
#====================

instanceURL <- "https://mlcinsurance.my.salesforce.com/"
sf_auth(login_url = instanceURL)

source('lib/soql_ocr_updated.R')
source('lib/soql_ccr.R')
source('lib/soql_acr_prod.R')

#=============================
# 2.0 Run additional tables ##
#=============================

rep_start <-ymd('2022-01-01')
rep_end <- rollback(Sys.Date()) #update the date to last day of previous month
rep_period <- interval(ymd(rep_start), ymd(rep_end))


soqlPayments <- paste("
SELECT cve__BenefitClaimed__r.Name, cve__PaymentContribution__c.Name, cve__Amount__c, cve__Payment__r.cve__Net__c, 
cve__Start__c, cve__Through__c, cve__Payment__r.cve__Release__c,cve__BenefitClaimed__r.cve__Claim__r.Name
FROM cve__PaymentContribution__c
WHERE cve__BenefitClaimed__r.cve__Claim__r.BusinessType__c = 'Group'
AND cve__Payment__r.cve__Status__c = 'Processed'",
                      "AND cve__Payment__r.cve__Release__c <= ", rep_end,
                      #"AND Name NOT IN ('Taxes','(Adjustment) Manual Adjustment')"
                      "")
df_payments <- salesforcer::sf_query(soql = soqlPayments, object_name = 'cve__PaymentContribution__c', api_type = 'Bulk 1.0', guess_types = FALSE)
df_payments <- df_payments %>% 
  mutate_at(vars('cve__Amount__c', 'cve__Payment__r.cve__Net__c'), funs(as.numeric)) %>% 
  mutate_at(vars('cve__Start__c', 'cve__Through__c', 'cve__Payment__r.cve__Release__c'), funs(ymd))

soqlDiagnosis <- paste(
  "SELECT Name, cve__MedicalCode__c, cve__MedicalCodeDescription__c, cve__DurationFactor__c, cve__Type__c,",
  "cve__Journal__r.cve__Claim__r.Name, StartDateOfDiagnosis__c, CreatedDate",
  "FROM cve__Diagnosis__c",
  "WHERE cve__Type__c IN ('Primary', 'Cause of Death')",
  "AND cve__Journal__r.cve__Claim__r.BusinessType__c = 'Group'",
  "")
df_diagnosis <- salesforcer::sf_query(soql = soqlDiagnosis, object_name = 'cve__Diagnosis__c', api_type = 'Bulk 1.0', guess_types = FALSE)
df_diagnosis <- df_diagnosis %>% 
  mutate_at(vars('StartDateOfDiagnosis__c'), funs(ymd)) %>% 
  mutate_at(vars('CreatedDate'), funs(as.Date))

#=============================
# 3.0 Call Claims Scripts ####
#=============================

# temp for Gayatri
#######################################
soqlOpportunity <- paste("SELECT id FROM Opportunity")
df_opportunity <- salesforcer::sf_query(soql = soqlOpportunity, object_name = 'Opportunity', api_type = 'Bulk 1.0', guess_types = FALSE)
############################################

ocr(rep_end)
ccr(rep_start,rep_end)
acr(rep_start,rep_end)




# config <- yaml.load_file('C:\\Users\\p290086\\config.yml')
#config <- yaml.load_file('C:/Users/vishesh.marwah/config_prod.yml')

#instanceURL <- "https://login.salesforce.com/"
#apiVersion <- "45.0"

#sf_auth(username = config$login$username, password = config$login$password, login_url = instanceURL)

###################################################################################################

# Authentication ----------------------------------------------------------

# The first time you run the code, R opens a browser session and has you log in.
# It then creates a file called .httr-oauth-salesforcer, which it uses for
# subsequent connections.

# Note that if this fails with:
# Error in oauth2.0_access_token(endpoint, app, code = code, user_params = user_params, :
#  Bad Request (HTTP 400). Failed to get an access token.
# you don't have the correct permissions.
# See https://confluence.mlctech.io/display/DA/Accessing+CV+from+R




