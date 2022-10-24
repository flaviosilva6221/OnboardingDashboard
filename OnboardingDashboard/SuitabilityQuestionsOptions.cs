using System;
using System.Collections.Generic;
using System.Text;

namespace OnboardingDashboard
{
    public class SuitabilityQuestionsOptions
    {
        public string Id { get; set; }
        public string Content { get; set; }
        public float Score { get; set; }
        public IEnumerable<SuitabilityQuestions> SubQuestions { get; set; }

        public SuitabilityQuestionsOptions() { }//Serialization
        public SuitabilityQuestionsOptions(string id, string content, float score, IEnumerable<SuitabilityQuestions> subQuestions)
        {
            Id = id;
            Content = content;
            Score = score;
            SubQuestions = subQuestions;
        }
    }
}
