using System;
using System.Collections.Generic;
using System.Text;

namespace OnboardingDashboard
{
    public class CustomerSuitabilityHistoryAnswers
    {
        public Guid SuitabilityId { get; set; }
        public DateTime AnsweredAt { get; set; }
        public string Profile { get; set; }
        public float Score { get; set; }
        public IEnumerable<CustomerSuitabilityQuestionAnswer> Answers { get; set; }
    }
}
