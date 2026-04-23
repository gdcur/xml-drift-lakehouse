# Schema Discovery Report

Source: `/home/gianfranco/dev/xml-drift-lakehouse/data/sample`

## Variant Distribution

| Variant | Files | % |
|---------|------:|--:|
| DetailedInvoice | 938 | 60.2% |
| SummaryInvoice | 620 | 39.8% |
| **Total** | **1558** | **100%** |

## Field Coverage by Variant

> % = files where field is populated / files where field is present

| Field | DetailedInvoice | SummaryInvoice |
|-------|------:|------:|
| `AccountCode` | 200% (21/1558) | 400% (2/1558) |
| `AccountInfo` | 0% (21/1558) | 0% (2/1558) |
| `AccountMajor` | 0% (21/1558) | 0% (2/1558) |
| `AccountMinor` | 0% (21/1558) | 0% (2/1558) |
| `AccountType` | 100% (21/1558) | 200% (2/1558) |
| `Action` | 0% (938/1558) | 0% (620/1558) |
| `ActionSource` | 0% (938/1558) | 0% (620/1558) |
| `ActionType` | 100% (938/1558) | 100% (620/1558) |
| `Address` | 0% (938/1558) | 0% (620/1558) |
| `AddressLine` | 100% (938/1558) | 100% (620/1558) |
| `Allocation` | 0% (938/1558) | 0% (620/1558) |
| `AllocationRate` | 212% (938/1558) | 300% (620/1558) |
| `Attachment` | 0% (938/1558) | 0% (620/1558) |
| `Attachments` | 0% (938/1558) | 0% (620/1558) |
| `Category` | 0% (929/1558) | 0% (620/1558) |
| `CategoryCode` | 100% (929/1558) | 100% (620/1558) |
| `ChargeClass` | 100% (938/1558) | 100% (620/1558) |
| `City` | 100% (938/1558) | 100% (620/1558) |
| `Contact` | 0% (938/1558) | 0% (620/1558) |
| `ContactNameOrLocation` | 100% (22/1558) | — |
| `ContentType` | 100% (938/1558) | 100% (620/1558) |
| `CostCenter` | 100% (21/1558) | 200% (2/1558) |
| `Country` | 100% (938/1558) | 100% (620/1558) |
| `CountryCode` | 100% (938/1558) | 100% (620/1558) |
| `CrossReference` | 0% (152/1558) | — |
| `CurrencyCode` | 100% (938/1558) | 100% (620/1558) |
| `CurrentOwner` | 0% (938/1558) | 0% (620/1558) |
| `Date` | 100% (2/1558) | — |
| `DaysDue` | 100% (3/1558) | — |
| `Department` | 0% (938/1558) | 0% (620/1558) |
| `DepartmentName` | 298% (938/1558) | 300% (620/1558) |
| `Description` | 373% (22/1558) | 400% (2/1558) |
| `DetailedInvoice` | 0% (938/1558) | — |
| `Discount` | 0% (938/1558) | 0% (620/1558) |
| `DiscountTotal` | 100% (938/1558) | 100% (620/1558) |
| `DocumentDate` | 100% (938/1558) | 100% (620/1558) |
| `DocumentHeader` | 0% (938/1558) | 0% (620/1558) |
| `DocumentLines` | 0% (938/1558) | 0% (620/1558) |
| `DocumentNumber` | 118% (938/1558) | 100% (620/1558) |
| `DocumentType` | 118% (938/1558) | 100% (620/1558) |
| `DueDate` | 100% (3/1558) | — |
| `EarlyPayment` | 0% (3/1558) | — |
| `EligibleFlag` | 100% (3/1558) | — |
| `EmailAddress` | 255% (938/1558) | 247% (620/1558) |
| `Entity` | 0% (938/1558) | 0% (620/1558) |
| `EntityCode` | 100% (938/1558) | 100% (620/1558) |
| `EntityName` | 102% (938/1558) | 100% (620/1558) |
| `ExemptCode` | 100% (103/1558) | — |
| `FirstName` | 204% (938/1558) | 200% (620/1558) |
| `Initial` | 340% (5/1558) | — |
| `LastName` | 204% (938/1558) | 200% (620/1558) |
| `LineCount` | 100% (938/1558) | 100% (620/1558) |
| `LineEntry` | 0% (938/1558) | 0% (620/1558) |
| `LineNumber` | 100% (938/1558) | 100% (620/1558) |
| `LinePretaxTotal` | 100% (938/1558) | — |
| `LineSubTotal` | 100% (938/1558) | — |
| `Location` | 0% (938/1558) | 0% (620/1558) |
| `LocationCode` | 299% (938/1558) | 300% (620/1558) |
| `LocationName` | 300% (938/1558) | 300% (620/1558) |
| `LocationPath` | 100% (938/1558) | 100% (620/1558) |
| `Metadata` | 100% (21/1558) | 200% (2/1558) |
| `Notes` | 298% (938/1558) | 300% (620/1558) |
| `Notice` | 0% (15/1558) | — |
| `NoticeType` | 100% (15/1558) | — |
| `OrderLineRef` | 100% (1/1558) | — |
| `OrderReference` | 100% (44/1558) | — |
| `Party` | 0% (938/1558) | 0% (620/1558) |
| `PeriodDate` | 123% (494/1558) | 200% (427/1558) |
| `PeriodStartDate` | 100% (494/1558) | — |
| `PhoneNumber` | 106% (918/1558) | 101% (488/1558) |
| `PostalZipCode` | 100% (938/1558) | 100% (620/1558) |
| `ProductCategory` | 100% (12/1558) | — |
| `ProductDescription` | 100% (938/1558) | — |
| `ProjectCode` | 100% (7/1558) | — |
| `PurchaseCategory` | 100% (2/1558) | — |
| `Quantity` | 100% (938/1558) | 100% (620/1558) |
| `RefData` | 0% (21/1558) | 0% (2/1558) |
| `RefInfo` | 0% (12/1558) | — |
| `RequestedBy` | — | 100% (2/1558) |
| `ServiceCode` | 100% (644/1558) | — |
| `ServiceItem` | 0% (938/1558) | — |
| `SiteRefName` | 100% (14/1558) | 200% (2/1558) |
| `SiteReference` | 0% (181/1558) | 0% (2/1558) |
| `StandardBillingDocument` | 0% (938/1558) | 0% (620/1558) |
| `StateProvince` | 100% (938/1558) | 100% (620/1558) |
| `StateProvinceCode` | 100% (938/1558) | 100% (620/1558) |
| `Status` | 100% (938/1558) | 100% (620/1558) |
| `SubmissionMethod` | 100% (938/1558) | 100% (620/1558) |
| `SummaryInvoice` | — | 0% (620/1558) |
| `TaxEntry` | 0% (107/1558) | — |
| `TaxType` | 100% (107/1558) | — |
| `Total` | 412% (938/1558) | 400% (620/1558) |
| `TransactionDateTime` | 100% (938/1558) | 100% (620/1558) |
| `TransactionIdentifier` | 100% (938/1558) | 100% (620/1558) |
| `UnitPrice` | 100% (938/1558) | 100% (620/1558) |
| `Units` | 100% (515/1558) | 100% (2/1558) |
| `Value` | 100% (21/1558) | 200% (2/1558) |
| `VendorTotal` | 100% (938/1558) | 100% (620/1558) |
| `WorkOrder` | 100% (72/1558) | — |

## Structural Notes

- Max line items per invoice: **2**
- Max allocations per line: **1**
- SummaryInvoice files with header-level Allocation: **620**

## ✅ No Unexpected Elements

All elements match known schema.

## Attribute Inventory

### DetailedInvoice

| Element | Attributes |
|---------|-----------|
| `ActionSource` | `role` |
| `Address` | `AddressType` |
| `Attachment` | `id`, `type` |
| `Attachments` | `numOfAttachments` |
| `Category` | `CategorizationScheme` |
| `Contact` | `Role`, `contactType` |
| `DetailedInvoice` | `id` |
| `EntityCode` | `EntityCodeType` |
| `LocationCode` | `LocationCodeType`, `siteCodeIndicator` |
| `Metadata` | `internal_code` |
| `Notice` | `code` |
| `Party` | `PartyRole` |
| `PhoneNumber` | `PhoneNumberType`, `phoneNumberIndicator` |
| `RefData` | `code`, `name`, `type` |
| `RefInfo` | `definitionOfOther`, `referenceInformationIndicator` |
| `ServiceCode` | `codeIndicator` |
| `SiteReference` | `locationType` |
| `StandardBillingDocument` | `type` |
### SummaryInvoice

| Element | Attributes |
|---------|-----------|
| `ActionSource` | `role` |
| `Address` | `AddressType` |
| `Attachment` | `id`, `type` |
| `Attachments` | `numOfAttachments` |
| `Category` | `CategorizationScheme` |
| `Contact` | `Role` |
| `EntityCode` | `EntityCodeType` |
| `LocationCode` | `LocationCodeType`, `siteCodeIndicator` |
| `Metadata` | `internal_code` |
| `Party` | `PartyRole` |
| `PhoneNumber` | `PhoneNumberType`, `phoneNumberIndicator` |
| `RefData` | `code`, `name`, `type` |
| `SiteReference` | `locationType` |
| `StandardBillingDocument` | `type` |
| `SummaryInvoice` | `id` |