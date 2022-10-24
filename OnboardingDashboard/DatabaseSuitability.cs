using System;

namespace OnboardingDashboard
{
    public class DatabaseSuitability
    {
        public string CustomerId { get; set; }
        public DateTime ChangedDate { get; set; }
        public string SuitabilityId { get; set; }
        public CustomerProfileTypeEnum CutomerProfileType { get; set; }
        public string CustomerProfile { get; set; }
        public string LastCustomerProfile { get; set; }
        public int? TotalOfCustomerSuitability { get; set; }
        public int? SuitabilityPosition { get; set; }
        public DateTime LastDate { get; set; }
        public string TermInfestorQualified { get; set; }
        public string FirstTimeAnsweredTerm { get; set; }
        public string FistTimeAnsweredSuitability { get; set; }

        public DatabaseSuitability() { } //Serializer
        public DatabaseSuitability(string customerId, string customerProfile, DateTime changedDate, string suitabilityId, CustomerProfileTypeEnum cutomerProfileType)
        {
            CustomerId = customerId;
            CustomerProfile = customerProfile;
            ChangedDate = changedDate;
            SuitabilityId = suitabilityId;
            CutomerProfileType = cutomerProfileType;
        }
    }
}
