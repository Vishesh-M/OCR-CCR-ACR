ocr <- function(rep_end) {


#rep_date <- '2022-10-31' #update to last date of previous month
# rep_date <- as.character(date)
soqlOCR <- paste(
  "SELECT ProductName__c,", # Product
  "cve__Claim__r.cve__Policy__r.Scheme__r.Name,", # Scheme Number
  "cve__Claim__r.cve__Policy__r.Scheme__r.SchemeName__c, ",
  "cve__Claim__r.cve__Policy__r.Scheme__r.Organisation__r.Name,", # Scheme Name
  "cve__Claim__r.cve__Policy__r.LegacyPASMemberNumber__c, cve__Claim__r.cve__Policy__r.Name,",
  "cve__Claim__r.cve__Policy__r.MemberNumber__c, cve__Claim__r.cve__Policy__r.pasMemberId__c,", # Member Number
  "cve__Claim__r.cve__ClaimantInsured__r.FirstName,", # Customer Name (First)
  "cve__Claim__r.cve__ClaimantInsured__r.LastName,", # Customer Name (Last)
  "cve__Claim__r.cve__ClaimantInsured__r.Birthdate,", # Date of Birth
  "cve__Claim__r.cve__Policy__r.DateJoinedFund__c,", # Date Joined Fund
  "cve__Benefit__r.BenefitCategory__c, EventType__c,", # Claim Type
  "", # Member Status N/A
  "cve__Claim__r.cve__ClaimantInsured__r.cve__Gender__c,", # Gender
  "", # Occupation Class
  "", # Hazardous Occupation,
  "cve__DateOfDisability__c,", # Date Incurred
  "WaitingPeriod__c, WaitingPeriodExtension__c, WaitingUnits__c,", # Waiting Period
  # Payments
  "cve__BenefitAmount__c, cve__CoverageAmount__c,", # All Sum Insured
  "BenefitStatusRecorded__c, cve__Status__c, ClosedReason__c, cve__Claim__r.cve__Status__c,", # Claim Status
  "cve__Claim__r.Owner.Name,", # Claim Assessor
  # Cause of Claim
  "NotificationDate__c, cve__Claim__r.CreatedDate,", # Date Notified of Claim
  "cve__Claim__r.ClaimFormReceivedDate__c, InitialUnderAssessmentDate__c,", # Claims Notified-Claim Form Received
  "BenefitPeriodPiclist__c,", # GSC Benefit Period
  "ICBECBComments__c, NextIndexationDate__c, IndexationDate__c,", # FSC Indexation/Escalation (contains the information, in some instances)
  "Reopened__c,", # Possible Reopened Claim
  "ExpectedClaimClosureDate__c,", # Expected Closure Date
  "", # Expected Closure Date Eff Date
  "LastModifiedBy.Name,", # Last Touched User
  "EndOfBenefitPeriod__c,", # End of Benefit Period
  "", # Claim Form Received System Date = Claims Notified-Claim Form Received
  "", # report run date
  "", # Previous Product,
  "", # Pay From Source
  "", # Advisor Code
  "", # MKSF Default Insurance - Death
  "", # MKSF Default Insurance - TPD
  
  "Id,", # for joins
  "cve__Claim__r.ClaimType__c, cve__Claim__r.TypeOfClaim__c,",
  "Name,", # for joins
  "cve__Claim__r.Name,", # for joins
  "ReopenedReason__c,",
  "BenefitClaimedClosureDate__c, ReopenedDate__c,", # for further scoping
  "cve__Benefit__r.Name,",# to merge Super Contributions
  "DateOfReoccurrence__c,", #Date of Re-occurrence
  "cve__Claim__r.CMT__c", # Claim Assessor Team
  "FROM cve__BenefitClaimed__c",
  "WHERE cve__Claim__r.BusinessType__c = 'Group'",
  "AND cve__Claim__r.ClaimType__c IN ('Claim', 'Post Claim')",
  "AND cve__Claim__r.cve__Status__c <> 'Incomplete'",
  "AND cve__Status__c <> 'Suspended'",
  # select only reported claims
  # "AND NotificationDate__c <= ", rep_date,
  "AND (cve__Claim__r.ClaimFormReceivedDate__c <=", rep_end,
  "OR InitialUnderAssessmentDate__c <=", rep_end,
  "OR NotificationDate__c <=", rep_end ,")",
  "AND (ClosedReason__c NOT IN ('Duplicate Claim in the Administration System','Not a Legitimate Benefit') OR ClosedReason__c = '')",
  "")

df_ocr <- salesforcer::sf_query(soql = soqlOCR, object_name = 'cve__BenefitClaimed__c', api_type = 'Bulk 1.0', guess_types = FALSE)


# Fix columns (API drops the column if there is all NA)
if (nrow(df_ocr) == 0) {
  cols <- c(str_remove(str_extract_all(soqlACR, '\\w+,', simplify = T), ','), 'cve__Benefit__r.Name')
  df_ocr_clean <- data.frame(matrix(ncol = length(cols), nrow = 0))
  colnames(df_ocr_clean) <- cols
  
  df_ocr_clean %>% 
    write_csv(sprintf('outputs/OCR_PreProd_Prototype_%s_%s.csv', 
                      rep_start, rep_end), na='')
  
  print('NO RECORDS! REPORT EXPORTED!')
  
}

df_ocr_clean <- df_ocr %>% 
  #mutate(ReopenedDate__c=parse_iso_8601(ReopenedDate__c, default_tz = "UTC")) %>% 
  # fix data types
  mutate_at(vars('ReopenedDate__c', 'cve__Claim__r.CreatedDate', 'cve__DateOfDisability__c', 'NotificationDate__c'), funs(as.Date)) %>% 
  mutate_at(vars('BenefitClaimedClosureDate__c', 'ReopenedDate__c', 'DateOfReoccurrence__c'), funs(ymd)) %>% 
  mutate_at(vars('cve__BenefitAmount__c', 'WaitingPeriod__c', 'WaitingPeriodExtension__c', 'cve__CoverageAmount__c'), funs(as.numeric)) %>% 
  # SC merge key
  mutate(`Date Incurred` = coalesce(cve__DateOfDisability__c),
         `Waiting Period Total` = WaitingPeriod__c + WaitingPeriodExtension__c,
         # key to merge additional SC with additional DII only
         add.aux = case_when(grepl('Additional', cve__Benefit__r.Name) ~ 'add',
                         TRUE ~ 'nonadd'),
         sc_key = sprintf('%s-%s-%s', cve__Claim__r.Name, `Date Incurred`, cve__Benefit__r.BenefitCategory__c),
         add_key = sprintf('%s-%s-%s', cve__Claim__r.Name, `Date Incurred`, cve__Benefit__r.BenefitCategory__c)) %>% 
  # remove the reopens that kept the old closure date
  filter( cve__Status__c != 'Closed' |############updated on 01-10-21
    # generic filter
    #!(ReopenedReason__c %in% c('Data Update', 'Data updated') & cve__Status__c = 'Closed')  &
    (is.na(BenefitClaimedClosureDate__c) | ############updated on 01-10-21
      BenefitClaimedClosureDate__c > rep_end ############updated on 01-10-21
      #(ReopenedDate__c >= BenefitClaimedClosureDate__c & ReopenedDate__c <= rep_end)) ############updated on 01-10-21
      # BenefitClaimedClosureDate__c > dmy('04-06-2019') |
      # (ReopenedDate__c > BenefitClaimedClosureDate__c & ReopenedDate__c <= dmy('04-06-2019'))
  )) %>%
  # join payments
  left_join(df_payments %>% 
              # mutate_at(vars('cve__Payment__r.cve__NetBeforeTaxes__c'), funs(as.numeric)) %>% 
              # mutate_at(vars('cve__Payment__r.cve__Start__c', 'cve__Payment__r.cve__Through__c', 'cve__Payment__r.cve__Release__c'), funs(ymd)) %>% 
              group_by(cve__BenefitClaimed__r.Name) %>% 
              summarise(TotalPaid = sum(cve__Amount__c),
                        FirstPayment = min(coalesce(cve__Start__c, cve__Payment__r.cve__Release__c)),
                        LastPayment = max(coalesce(cve__Through__c, cve__Payment__r.cve__Release__c)),
                        LastAmount = cve__Amount__c[cve__Payment__r.cve__Release__c == max(cve__Payment__r.cve__Release__c)][1],
                        EffDateLastPayment = max(cve__Payment__r.cve__Release__c)), 
            by=c('Name' = 'cve__BenefitClaimed__r.Name')) %>% 
  # join diagnosis (Only 1 primary)
  left_join(df_diagnosis %>% 
              mutate(aux = coalesce(StartDateOfDiagnosis__c, CreatedDate)) %>% 
              group_by(cve__Journal__r.cve__Claim__r.Name) %>% 
              slice(which.max(aux)) %>%  # ger most recent primary diagnosis
              ungroup() %>% 
              select(cve__Journal__r.cve__Claim__r.Name, cve__MedicalCode__c, cve__MedicalCodeDescription__c, cve__DurationFactor__c),
            by=c('cve__Claim__r.Name' = 'cve__Journal__r.cve__Claim__r.Name'))

# sc <- df_ocr_clean %>%
#   filter(grepl('Super Contribution', cve__Benefit__r.Name)) %>%
#   rename(BenefitSC = cve__CoverageAmount__c, TotalPaidSC = TotalPaid) %>%
#   select(cve__Claim__r.Name, BenefitSC, TotalPaidSC)

df_ocr_clean1 <- df_ocr_clean %>%
#   # merge super contributions
  filter(!(grepl('Super Contribution', cve__Benefit__r.Name))) %>%
  left_join(
    df_ocr_clean %>%
      filter(grepl('^Super Contribution', cve__Benefit__r.Name)) %>%
      rename(BenefitSC = cve__CoverageAmount__c, TotalPaidSC = TotalPaid) %>%
      select(sc_key, BenefitSC, TotalPaidSC),
    by=c('sc_key')
  ) %>%
   
#   # merge additional super contributions
  filter(!grepl('Additional Super Contribution', cve__Benefit__r.Name)) %>%
  left_join(
    df_ocr_clean %>%
      filter(grepl('Additional Super Contribution', cve__Benefit__r.Name)) %>%
      rename(BenefitSCAdd = cve__CoverageAmount__c, TotalPaidSCAdd = TotalPaid) %>%
      select(sc_key, BenefitSCAdd, TotalPaidSCAdd),
    by=c('sc_key')
  ) %>%
#   # merge Additional Benefit
  filter(!grepl('^Additional', cve__Benefit__r.Name)) %>%
  left_join(
    df_ocr_clean %>%
      filter(grepl('^Additional (?!(Super Contribution))', cve__Benefit__r.Name, perl=T)) %>%
      rename(BenefitAdd = cve__CoverageAmount__c, TotalPaidAdd = TotalPaid) %>%
      select(add_key, BenefitAdd, TotalPaidAdd),
    by=c('add_key')
  )
  #########################################################################################################
  #sc_open %>% select( c(62:121))


  sc_open <- df_ocr_clean %>% 
    filter(grepl('^Super Contribution', cve__Benefit__r.Name)) %>%
    left_join(
      df_ocr_clean %>% 
        filter(!(grepl('^Super Contribution', cve__Benefit__r.Name))) %>% 
        rename(BenefitSC = cve__CoverageAmount__c, TotalPaidSC = TotalPaid) %>% 
        select(sc_key, BenefitSC, TotalPaidSC), 
      by = c('sc_key')
      )
    
    add_open <-  df_ocr_clean %>% 
    filter(grepl('Additional Super Contribution', cve__Benefit__r.Name)) %>%
    left_join(
      df_ocr_clean %>%
        filter(!(grepl('Additional Super Contribution', cve__Benefit__r.Name))) %>%
        rename(BenefitSCAdd = cve__CoverageAmount__c, TotalPaidSCAdd = TotalPaid) %>%
        select(sc_key, BenefitSCAdd, TotalPaidSCAdd),
      by=c('sc_key')
    )
  
  sc_add_open <- df_ocr_clean %>% 
    filter(grepl('^Additional', cve__Benefit__r.Name)) %>%
    left_join(
      df_ocr_clean %>%
        filter(!(grepl('^Additional (?!(Super Contribution))', cve__Benefit__r.Name, perl=T))) %>%
        rename(BenefitAdd = cve__CoverageAmount__c, TotalPaidAdd = TotalPaid) %>%
        select(add_key, BenefitAdd, TotalPaidAdd),
      by=c('add_key')
    )
  
  merged_ocr <- bind_rows(sc_open,add_open,sc_add_open) %>% filter(!cve__Claim__r.Name %in% df_ocr_clean1$cve__Claim__r.Name)
  
  df_ocr_clean2 <- df_ocr_clean1 %>% 
    rbind(merged_ocr) %>% 
    mutate(BenefitSC = BenefitSC * (grepl('Income Protection', cve__Benefit__r.Name)), # dont consider SC for non-DII claims
         # BenefitSCAdd = BenefitSCAdd * (grepl('^Additional (?!(Super Contribution))', cve__Benefit__r.Name, perl=T)), # dont consider Additional SC for non-Additional DII claims
         `Sum Insured` = cve__CoverageAmount__c,
         `Sum Insured Combined` = (cve__CoverageAmount__c + replace_na(BenefitAdd,0) + replace_na(BenefitSC, 0) + replace_na(BenefitSCAdd, 0)) * 
           ifelse(grepl('Income Protection', cve__Benefit__r.Name), 12, 1), # make SC SI annual
         TotalPaid = TotalPaid + replace_na(TotalPaidSC,0),
         `Source of Claim` = coalesce(cve__MedicalCodeDescription__c, cve__DurationFactor__c, cve__MedicalCode__c),
         `Total Claim Start Day (GSC)` = case_when(grepl('Income Protection', cve__Benefit__r.Name) ~ FirstPayment,
                                                   TRUE ~ as.Date(NA)),
         `Total Claim End Day (GSC)` = case_when(grepl('Income Protection', cve__Benefit__r.Name) ~ LastPayment,
                                                 TRUE ~ as.Date(NA)),
         `No. of Days on Claim (GSC)` = case_when(grepl('Income Protection', cve__Benefit__r.Name) ~ 
                                                    interval(FirstPayment, LastPayment) / days(1),
                                                  TRUE ~ NA_real_),
         `Date Notified of Claim` = NotificationDate__c,
         `Date Received` = NotificationDate__c,
         `report run date` = rep_end) %>% 
  rename(`Product Variant` = ProductName__c,
         `Scheme Number` = cve__Claim__r.cve__Policy__r.Scheme__r.Name,
         `Scheme Name` = cve__Claim__r.cve__Policy__r.Scheme__r.SchemeName__c,
         `First Name` = cve__Claim__r.cve__ClaimantInsured__r.FirstName,
         `Last Name` = cve__Claim__r.cve__ClaimantInsured__r.LastName,
         `Date Of Birth` = cve__Claim__r.cve__ClaimantInsured__r.Birthdate,
         `Date Joined Fund` = cve__Claim__r.cve__Policy__r.DateJoinedFund__c,
         `Benefit Name` = cve__Benefit__r.Name,
         `Benefit Category` = cve__Benefit__r.BenefitCategory__c,
         `Event Type` = EventType__c,
         Gender = cve__Claim__r.cve__ClaimantInsured__r.cve__Gender__c,
         `Total PAYG` = TotalPaid,
         `Effective Date of Latest Payment` = EffDateLastPayment,
         `Latest payment amount` = LastAmount,
         `Closed Reason Category` = BenefitStatusRecorded__c,
         `Case Status` = cve__Claim__r.cve__Status__c,
         `BC Status` = cve__Status__c,
         `Closed Reason` = ClosedReason__c,
         `Claim Assessor` = cve__Claim__r.Owner.Name,
         `Claims Notified-Claim Form Received` = cve__Claim__r.ClaimFormReceivedDate__c,
         `GSC Benefit Period` = BenefitPeriodPiclist__c,
         `Next Indexation Date` = IndexationDate__c,
         `Possible Reopened Claim` = Reopened__c,
         `Expected Closure Date` = ExpectedClaimClosureDate__c,
         `Last Touched User` = LastModifiedBy.Name,
         `End of Benefit Period` = EndOfBenefitPeriod__c,
         `Legacy PAS Member Number` = cve__Claim__r.cve__Policy__r.LegacyPASMemberNumber__c,
         `Super Fund Member Number` = cve__Claim__r.cve__Policy__r.MemberNumber__c,
         `PAS Member Number` = cve__Claim__r.cve__Policy__r.pasMemberId__c,
         `Case Number` = cve__Claim__r.Name,
         `Organisation` = cve__Claim__r.cve__Policy__r.Scheme__r.Organisation__r.Name,
         `Waiting Units` = WaitingUnits__c,
         `Waiting Period` = WaitingPeriod__c,
         `Waiting Period Extension` = WaitingPeriodExtension__c,
         `Policy` = cve__Claim__r.cve__Policy__r.Name,
         `BC` = Name,
         `Reoccurrence Date` = DateOfReoccurrence__c,
         `Reopened Reason` = ReopenedReason__c,
         ICD10 = cve__MedicalCode__c,
         CaseOwnerTeam = cve__Claim__r.CMT__c) %>% 
  select(`Product Variant`,
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
         `Date Joined Fund`,
         `Benefit Name`,
         `Benefit Category`,
         `Case Status`,
         `BC Status`,
         `Closed Reason`,
         `Closed Reason Category`,
         `Gender`,
         `Date Incurred`,
         `Waiting Period`,
         `Waiting Period Extension`,
         `Waiting Period Total`,
         `Waiting Units`,
         `Total PAYG`,
         `Total Claim Start Day (GSC)`,
         `Total Claim End Day (GSC)`,
         `No. of Days on Claim (GSC)`,
         `Effective Date of Latest Payment`,
         `Latest payment amount`,
         `Sum Insured`,
         BenefitAdd,
         BenefitSC,
         BenefitSCAdd,
         `Sum Insured Combined`,
         `Claim Assessor`,
         `Source of Claim`,
         `Date Notified of Claim`,
         `Date Received`,
         `GSC Benefit Period`,
         `Next Indexation Date`,
         `Possible Reopened Claim`,
         `Expected Closure Date`,
         `Last Touched User`,
         `End of Benefit Period`,
         `report run date`,
         `Policy`,
         `BC`,
         BenefitClaimedClosureDate__c,
         ReopenedDate__c,
         `Reoccurrence Date`,
         `Reopened Reason`,
         ICD10,
         CaseOwnerTeam)


df_ocr_clean2 %>% write_csv(sprintf('outputs/OCR_%s_modified.csv', rep_end), na='')

df_ocr_clean2 %>% write_csv(sprintf('C:/Users/Vishesh.Marwah/OneDrive - MLC Life Insurance/Group Claims Contingency Report/OCR_%s_modified.csv', rep_end), na='')

}
# df_ocr_clean1 %>% group_by(Name) %>% filter(n()>1)
#df_ocr_clean %>% write.csv(sprintf('outputs/OCR_PreProd_Prototype_%s.csv', rep_date), na='', quote = TRUE)
# }
