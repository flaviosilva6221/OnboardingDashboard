using System;
using System.Collections.Generic;
using System.Text;

namespace OnboardingDashboard
{
    public class CustomerTerms
    {
        // Primary key
        public string Id { get; set; }
        public string Type { get; set; }

        // Attributes
        public string Name { get; set; }
        public string Size { get; set; }
        public string Extension { get; set; }
        public string Url { get; set; }
        public string Html { get; set; }
        public DateTime CreatedDate { get; set; }
    }
}
