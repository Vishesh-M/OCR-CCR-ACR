acr <- function(rep_start,rep_end) {
  

rep_period <- interval(ymd(rep_start), ymd(rep_end))

output_folder_onedrive <- 'C:/Users/Vishesh.Marwah/OneDrive - MLC Life Insurance/Group Claims Contingency Report'

output_file_onedrive <- file.path(output_folder_onedrive, sprintf('ACR_%s_%s.csv', rep_start, rep_end))

  
soqlACR <- paste(
  # OCR Info
  "SELECT cve__BenefitClaimed__r.ProductName__c,", # Product
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Scheme__r.Name,", # Scheme Number
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Scheme__r.SchemeName__c, ",
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Scheme__r.Organisation__r.Name,", # Scheme Name
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.LegacyPASMemberNumber__c, ",
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Name,",
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.MemberNumber__c, ",
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.pasMemberId__c,", # Member Number
  "cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.FirstName,", # Customer Name (First)
  "cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.LastName,", # Customer Name (Last)
  "cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.Birthdate,", # Date of Birth
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.DateJoinedFund__c,", # Date Joined Fund
  "cve__BenefitClaimed__r.cve__Benefit__r.BenefitCategory__c,", # Claim Type
  "cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.cve__Gender__c,", # Gender
  "cve__BenefitClaimed__r.cve__DateOfDisability__c,", # Date Incurred
  "cve__BenefitClaimed__r.WaitingPeriod__c, cve__BenefitClaimed__r.WaitingUnits__c,", # Waiting Period
  "cve__BenefitClaimed__r.cve__CoverageAmount__c,", # All Sum Insured
  "cve__BenefitClaimed__r.BenefitStatusRecorded__c, cve__BenefitClaimed__r.cve__Status__c,",
  "cve__BenefitClaimed__r.ClosedReason__c, ",# Claim Status
  "cve__BenefitClaimed__r.cve__Claim__r.Owner.Name,", # Claim Assessor
  "cve__BenefitClaimed__r.NotificationDate__c,", # Date Notified of Claim
  "cve__BenefitClaimed__r.cve__Claim__r.ClaimFormReceivedDate__c, cve__BenefitClaimed__r.InitialUnderAssessmentDate__c,", # Claims Notified-Claim Form Received
  "cve__BenefitClaimed__r.BenefitPeriodPiclist__c,", # GSC Benefit Period
  "cve__BenefitClaimed__r.ICBECBComments__c,", # FSC Indexation/Escalation (contains the information, in some instances)
  "cve__BenefitClaimed__r.ExpectedClaimClosureDate__c,", # Expected Closure Date
  "cve__BenefitClaimed__r.EndOfBenefitPeriod__c,", # End of Benefit Period
  "cve__BenefitClaimed__r.BenefitClaimedClosureDate__c,", # Claim Finalised
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.InSuper__c,", # Super/Non Super
  "cve__BenefitClaimed__r.Reopened__c,",
  "cve__BenefitClaimed__r.cve__Claim__r.cve__Status__c,",
  "cve__BenefitClaimed__r.ReopenedReason__c,",
  "cve__BenefitClaimed__r.cve__Claim__r.Name,", # for joins
  "cve__BenefitClaimed__r.cve__Benefit__r.Name,", # for Super Contributions
  "cve__BenefitClaimed__r.Name,", # for sanity check
  "cve__Payment__r.Name, Id,", # for sanity check
  
  # Payment Info
  "Name,",
  "cve__Amount__c,",
  "cve__Start__c,",
  "cve__Through__c,",
  "cve__Payment__r.cve__Start__c,",
  "cve__Payment__r.cve__Release__c,", # Processed Date
  "cve__Payment__r.cve__Status__c,", # Payment Status
  "cve__Payment__r.PASReferenceNumber__c", # Tr Ref No
  
  "FROM cve__PaymentContribution__c",
  "WHERE cve__BenefitClaimed__r.cve__Claim__r.BusinessType__c = 'Group'",
  "AND cve__BenefitClaimed__r.cve__Claim__r.ClaimType__c IN ('Claim', 'Post Claim')",
  "AND cve__BenefitClaimed__r.cve__Claim__r.cve__Status__c <> 'Incomplete'",
  # gets payments based on Start Date (from contr or payment spec)
  #"AND ((cve__Start__c >=", rep_start, "AND cve__Start__c <=", rep_end, ")",
  #"OR (cve__Payment__r.cve__Start__c >=", rep_start, "AND cve__Payment__r.cve__Start__c <=", rep_end, "))",
   "AND cve__Payment__r.cve__Release__c >=", rep_start,
  # "AND cve__Payment__r.cve__Release__c <=", rep_end,
  "")
df_acr <- salesforcer::sf_query(soql = soqlACR, object_name = 'cve__PaymentContribution__c', api_type = 'Bulk 1.0', guess_types = FALSE)

df_acr_clean <- df_acr %>% 
  # fix data types
  mutate_at(vars('cve__BenefitClaimed__r.NotificationDate__c'), funs(as.Date)) %>% 
  mutate_at(vars('cve__BenefitClaimed__r.WaitingPeriod__c','cve__Amount__c','cve__BenefitClaimed__r.WaitingPeriod__c',
                 'cve__BenefitClaimed__r.cve__CoverageAmount__c'), 
            funs(as.numeric)) %>% 
  # join diagnosis (Only 1 primary)
  left_join(df_diagnosis %>% 
              mutate(aux = coalesce(StartDateOfDiagnosis__c, CreatedDate)) %>% 
              group_by(cve__Journal__r.cve__Claim__r.Name) %>% 
              slice(which.max(aux)) %>%  # ger most recent primary diagnosis
              ungroup() %>% 
              select(cve__Journal__r.cve__Claim__r.Name, cve__MedicalCode__c, cve__DurationFactor__c, cve__MedicalCodeDescription__c),
            by=c('cve__BenefitClaimed__r.cve__Claim__r.Name' = 'cve__Journal__r.cve__Claim__r.Name')) %>% 
  mutate(`Annual Sum Insured` = (cve__BenefitClaimed__r.cve__CoverageAmount__c) * 
           ifelse(grepl('Income Protection', cve__BenefitClaimed__r.cve__Benefit__r.Name), 12, 1), # make SC SI annual
         `Source of Claim` = coalesce(cve__MedicalCodeDescription__c, 
                                      cve__DurationFactor__c,
                                      cve__MedicalCode__c),
         `Date Incurred` = cve__BenefitClaimed__r.cve__DateOfDisability__c,
         `Date Notified of Claim` = cve__BenefitClaimed__r.NotificationDate__c,
         `Date Received` = cve__BenefitClaimed__r.NotificationDate__c) %>% 
  rename(`Product` = cve__BenefitClaimed__r.ProductName__c,
         `Scheme Number` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Scheme__r.Name,
         `Scheme Name` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Scheme__r.SchemeName__c,
         `Organisation` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Scheme__r.Organisation__r.Name,
         `Legacy PAS Member Number` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.LegacyPASMemberNumber__c,
         `Super Fund Member Number` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.MemberNumber__c,
         `PAS Member Number` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.pasMemberId__c,
         `Case Number` = cve__BenefitClaimed__r.cve__Claim__r.Name,
         `First Name` = cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.FirstName,
         `Last Name` = cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.LastName,
         `Date Of Birth` = cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.Birthdate,
         `Benefit Category` = cve__BenefitClaimed__r.cve__Benefit__r.BenefitCategory__c,
         Gender = cve__BenefitClaimed__r.cve__Claim__r.cve__ClaimantInsured__r.cve__Gender__c,
         `Super/Non Super` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.InSuper__c,  
         `Payment Contribution` = Name,
         `PAYG Amount` = cve__Amount__c, # For all claims
         `Payment Start` = cve__Start__c,
         `Payment End` = cve__Through__c,
         `Payment Status` = cve__Payment__r.cve__Status__c,
         `PAS Reference Number` = cve__Payment__r.PASReferenceNumber__c,
         `Payment Processed Date` = cve__Payment__r.cve__Release__c,
         `Payment Date: From` = cve__Payment__r.cve__Start__c,
         `System Closure Date` = cve__BenefitClaimed__r.BenefitClaimedClosureDate__c,
         `GSC Benefit Period` = cve__BenefitClaimed__r.BenefitPeriodPiclist__c,
         `Expected Closure Date` = cve__BenefitClaimed__r.ExpectedClaimClosureDate__c,
         `End of Benefit Period` = cve__BenefitClaimed__r.EndOfBenefitPeriod__c,
         `Waiting Units` = cve__BenefitClaimed__r.WaitingUnits__c,
         `Waiting Period` = cve__BenefitClaimed__r.WaitingPeriod__c,
         `Closed Reason Category` = cve__BenefitClaimed__r.BenefitStatusRecorded__c,
         `Case Status` = cve__BenefitClaimed__r.cve__Claim__r.cve__Status__c,
         `BC Status` = cve__BenefitClaimed__r.cve__Status__c,
         `Closed Reason` = cve__BenefitClaimed__r.ClosedReason__c,
         `BC` = cve__BenefitClaimed__r.Name,
         `Policy` = cve__BenefitClaimed__r.cve__Claim__r.cve__Policy__r.Name,
         `Possible Reopened Claim` = cve__BenefitClaimed__r.Reopened__c,
         ICD10 = cve__MedicalCode__c) %>% 
  select(`Policy`,
          `Product`,
         `Scheme Number`,
         `Scheme Name`,
         `Organisation`,
         `Case Number`,
         `Legacy PAS Member Number`,
         `Super Fund Member Number`,
         `PAS Member Number`,
         `First Name`,
         `Last Name`,
         `Date Of Birth`,
         `Benefit Category`,
         `Gender`,
         `Date Incurred`,
         `Super/Non Super`,
         `Waiting Period`,
         `Waiting Units`,
         `Annual Sum Insured`,
         `Payment Contribution`,
         `PAYG Amount`,
         `Payment Status`,
         `PAS Reference Number`,
         `Payment Processed Date`,
         `Payment Date: From`,
         `Payment Start`,
         `Payment End`,
         `System Closure Date`,
         `Source of Claim`,
         `Date Notified of Claim`,
         `GSC Benefit Period`,
         `Expected Closure Date`,
         `End of Benefit Period`,
         ICD10)

# Get the Indexation / Escalation from the lookup file (FAIL ATTEMPT, discrepancies from lookup and original ACR)
# library(readxl)
# scheme_info <- read_excel('inputs/Schema_Info.xlsx', skip=4)
# scheme_info <- scheme_info %>% 
#   filter(`Increasing Claim Benefit Applicable` == 'Yes') %>% 
#   mutate(`Maximum Increasing benefits/indexation Value - Default Benefit` = str_remove_all(`Maximum Increasing benefits/indexation Value - Default Benefit`, '%|\\$'),
#          `Maximum Increasing benefits/indexation Value - Override Benefit` = str_remove_all(`Maximum Increasing benefits/indexation Value - Override Benefit`, '%|\\$'),
#          IndexationEscalation = as.numeric(coalesce(`Maximum Increasing benefits/indexation Value - Default Benefit`, `Maximum Increasing benefits/indexation Value - Override Benefit`)),
#          IndexationEscalation = case_when(IndexationEscalation > 1 ~ IndexationEscalation / 100,
#                                           TRUE ~ IndexationEscalation)) %>% 
#   select(`Scheme Number`, IndexationEscalation) %>% 
#   group_by(`Scheme Number`) %>% 
#   slice(1) %>% 
#   ungroup()
# 
# df_acr_clean <- df_acr_clean %>% 
#   left_join(scheme_info, by=c('Scheme Number')) %>% 
#   mutate(`GS Indexation/Escalation` = case_when(!is.na(IndexationEscalation) ~ sprintf('Escalate at %s%%', IndexationEscalation*100),
#                                                 TRUE ~ NA_character_)) %>% 
#   select(-IndexationEscalation)

df_acr_clean %>% 
  write_csv(sprintf('outputs/ACR_Prod_Prototype_%s_%s.csv', 
                    rep_start, 
                    rep_end), 
            na='')

df_acr_clean %>% write_csv(output_file_onedrive, na='')

}
