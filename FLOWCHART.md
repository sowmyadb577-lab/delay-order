# Delay-Order Project Flowchart

## System Architecture Flow

```mermaid
graph TB
    Start([Start]) --> Login[Register/Login User<br/>User Interface]
    
    Login --> Branch{User Type}
    
    Branch -->|Path 1| Home[Home]
    Branch -->|Path 2| Railways[Railways]
    Branch -->|Path 3| Medicals[Medicals]
    
    Home --> Predict[Predict Delay]
    Predict --> AudioModel[Audio Processing Model]
    AudioModel --> MachineLearning[Machine Learning Model]
    MachineLearning --> ResultDisplay[Result Display]
    
    Railways --> ImageDetection[Image Detection]
    ImageDetection --> ImageProcessing[Image Processing<br/>Module]
    ImageProcessing --> MachineLearningImg[Machine Learning<br/>Model]
    MachineLearningImg --> ResultOutput[Result<br/>Output]
    
    Railways --> InfoPartner[Information<br/>about<br/>Partners]
    
    Medicals --> RetrieveContact[Retrieve Contact<br/>Details of<br/>Emergency<br/>Loghead<br/>Delivery]
    
    ResultDisplay --> Decision1{Delay?}
    Decision1 -->|No| Low1[Low]
    Decision1 -->|Yes| Decision2{Severity}
    Decision2 -->|No| High1[High]
    Decision2 -->|Yes| Mod1[Mod]
    High1 --> Alert[Alert]
    Mod1 --> Alert
    Low1 --> Alert
    
    ResultOutput --> Decision3{Delay?}
    Decision3 -->|No| Decision4{Severe?}
    Decision4 -->|No| High2[High]
    Decision4 -->|Yes| Low2[Low]
    Decision3 -->|Yes| Decision5{Severe?}
    Decision5 -->|No| Mod2[Mod]
    Decision5 -->|Yes| High3[High]
    
    High2 --> Result[Result]
    Low2 --> Result
    Mod2 --> Result
    High3 --> Result
    
    Alert --> LogHead1([Loghead])
    Result --> LogHead2([Loghead])
```

## Process Description

### 1. User Authentication
- System starts with user registration/login interface
- Users authenticate to access the system

### 2. Main Branches

#### **Home Path - Audio-based Delay Prediction**
- **Predict Delay**: Initiates delay prediction process
- **Audio Processing Model**: Processes audio input data
- **Machine Learning Model**: Analyzes patterns to predict delays
- **Result Display**: Shows prediction results
- **Decision Logic**:
  - Checks if delay exists (Yes/No)
  - Evaluates severity level
  - Categorizes as: Low, Moderate (Mod), or High
  - Triggers alert notifications
  - Logs to system (Loghead)

#### **Railways Path - Image-based Detection**
- **Image Detection**: Captures railway-related images
- **Image Processing Module**: Processes visual data
- **Machine Learning Model**: Analyzes images for delays
- **Information about Partners**: Provides partner details
- **Result Output**: Generates analysis results
- **Decision Logic**:
  - Determines delay presence
  - Assesses severity
  - Classifies priority level
  - Records results

#### **Medicals Path - Emergency Contact System**
- **Retrieve Contact Details**: Accesses emergency logistics contacts
- Manages delivery coordination for critical medical supplies
- Handles emergency loghead delivery information

### 3. Output & Logging
- All paths converge to **Loghead** logging system
- Results stored for analysis and reporting
- Alerts sent to relevant stakeholders

## Key Features

- **Multi-modal Analysis**: Supports both audio and image-based delay detection
- **Severity Classification**: Three-level priority system (Low, Moderate, High)
- **Emergency Handling**: Dedicated medical logistics pathway
- **Partner Integration**: Railway partner information system
- **Automated Alerts**: Real-time notification system
- **Comprehensive Logging**: Complete audit trail via Loghead system

## Technology Stack

- **Machine Learning**: Delay prediction and pattern recognition
- **Audio Processing**: Voice/sound analysis for delay detection
- **Image Processing**: Visual analysis for railway operations
- **User Management**: Authentication and authorization system
- **Logging System**: Centralized logging (Loghead)

---

*This flowchart represents the complete architecture of the delay-order delivery analysis system.*
