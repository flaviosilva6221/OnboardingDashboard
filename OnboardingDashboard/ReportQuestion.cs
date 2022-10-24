using System;

namespace OnboardingDashboard
{
    public class ReportQuestion
    {
        public string Customer { get; set; }
        public string Question { get; set; }
        public string answer { get; set; }
        public DateTime SuitabilityDate { get; set; }
        public string Profile { get; set; }
        public string SuitabilityScore { get; set; }
        public string Optionscore { get; set; }
    }
}