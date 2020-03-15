using System;
using System.Collections.Generic;
using System.Text;

namespace Demo.Data.Testing
{
    //derrived from several public domain sources and adapted to this utility
    public static class RandomPassword
    {
        static readonly Random _RandomNumberGenerator = RandomStuff.NewRandomGenerator();

        const int DEFAULT_MIN_PASSWORD_LENGTH = 8;
        const int DEFAULT_MAX_PASSWORD_LENGTH = 16;
        
        const string PASSWORD_CHARS_LCASE = @"abcdefgijkmnpqrstwxyz";
        const string PASSWORD_CHARS_UCASE = @"ABCDEFGHJKLMNPQRSTWXYZ";
        const string PASSWORD_CHARS_NUMERIC = @"123456789";
        const string PASSWORD_CHARS_SPECIAL = @"!@#$%^&*()_+-={}|[]\:;<>?/";

 


        public static string Generate()
        {
            return Generate(DEFAULT_MIN_PASSWORD_LENGTH, DEFAULT_MAX_PASSWORD_LENGTH, true);
        }

        public static string Generate(bool includeSpecialCharacters)
        {
            return Generate(DEFAULT_MIN_PASSWORD_LENGTH, DEFAULT_MAX_PASSWORD_LENGTH, includeSpecialCharacters);
        }

        public static string Generate(int length)
        {
            return Generate(length, length, true);
        }

        public static string Generate(int length, bool includeSpecialCharacters)
        {
            return Generate(length, length, includeSpecialCharacters);
        }

        public static string Generate(int minLength, int maxLength, bool includeSpecialCharacters)
        {
            char[][] charGroups;
            if (includeSpecialCharacters)
            {
                charGroups = new char[][] 
                {
                    PASSWORD_CHARS_LCASE.ToCharArray(),
                    PASSWORD_CHARS_UCASE.ToCharArray(),
                    PASSWORD_CHARS_NUMERIC.ToCharArray(),
                    PASSWORD_CHARS_SPECIAL.ToCharArray()
                };
            }
            else
            {
                charGroups = new char[][] 
                {
                    PASSWORD_CHARS_LCASE.ToCharArray(),
                    PASSWORD_CHARS_UCASE.ToCharArray(),
                    PASSWORD_CHARS_NUMERIC.ToCharArray()
                };
            }
            // Use this array to track the number of unused characters in each
            // character group.
            int[] charsLeftInGroup = new int[charGroups.Length];

            // Initially, all characters in each group are not used.
            for (int charIndex = 0; charIndex < charsLeftInGroup.Length; charIndex++)
            {
                charsLeftInGroup[charIndex] = charGroups[charIndex].Length;
            }
            // Use this array to track (iterate through) unused character groups.
            int[] leftGroupsOrder = new int[charGroups.Length];

            // Initially, all character groups are not used.
            for (int charIndex = 0; charIndex < leftGroupsOrder.Length; charIndex++)
            {
                leftGroupsOrder[charIndex] = charIndex;
            }

            // This array will hold password characters.
            char[] passwordChars = null;

            // Allocate appropriate memory for the password.
            if (minLength < maxLength)
            {
                passwordChars = new char[_RandomNumberGenerator.Next(minLength, maxLength + 1)];
            }
            else
            {
                passwordChars = new char[minLength];
            }

            int nextCharIdx;
            int nextGroupIdx;
            int nextLeftGroupsOrderIdx;
            int lastCharIdx;

            // Index of the last non-processed group.
            int lastLeftGroupsOrderIdx = leftGroupsOrder.Length - 1;

            // Generate password characters one at a time.
            for (int charIndex = 0; charIndex < passwordChars.Length; charIndex++)
            {

                if (lastLeftGroupsOrderIdx == 0)
                {
                    nextLeftGroupsOrderIdx = 0;
                }
                else
                {
                    nextLeftGroupsOrderIdx = _RandomNumberGenerator.Next(0, lastLeftGroupsOrderIdx);
                }

                nextGroupIdx = leftGroupsOrder[nextLeftGroupsOrderIdx];

                lastCharIdx = charsLeftInGroup[nextGroupIdx] - 1;

                if (lastCharIdx == 0)
                {
                    nextCharIdx = 0;
                }
                else
                {
                    nextCharIdx = _RandomNumberGenerator.Next(0, lastCharIdx + 1);
                }
                // Add this character to the password.
                passwordChars[charIndex] = charGroups[nextGroupIdx][nextCharIdx];

                // If we processed the last character in this group, start over.
                if (lastCharIdx == 0)
                {
                    charsLeftInGroup[nextGroupIdx] = charGroups[nextGroupIdx].Length;
                }

                else
                {

                    if (lastCharIdx != nextCharIdx)
                    {
                        char temp = charGroups[nextGroupIdx][lastCharIdx];
                        charGroups[nextGroupIdx][lastCharIdx] = charGroups[nextGroupIdx][nextCharIdx];
                        charGroups[nextGroupIdx][nextCharIdx] = temp;
                    }

                    charsLeftInGroup[nextGroupIdx]--;
                }


                if (lastLeftGroupsOrderIdx == 0)
                {
                    lastLeftGroupsOrderIdx = leftGroupsOrder.Length - 1;
                }
                // There are more unprocessed groups left.
                else
                {
                    // Swap processed group with the last unprocessed group
                    // so that we don't pick it until we process all groups.
                    if (lastLeftGroupsOrderIdx != nextLeftGroupsOrderIdx)
                    {
                        int temp = leftGroupsOrder[lastLeftGroupsOrderIdx];
                        leftGroupsOrder[lastLeftGroupsOrderIdx] = leftGroupsOrder[nextLeftGroupsOrderIdx];
                        leftGroupsOrder[nextLeftGroupsOrderIdx] = temp;
                    }
                    // Decrement the number of unprocessed groups.
                    lastLeftGroupsOrderIdx--;
                }
            }

            // Convert password characters into a string and return the result.
            return new string(passwordChars);
        }

    }
}
