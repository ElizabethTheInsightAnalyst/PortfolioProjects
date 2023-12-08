Exploring COVID-19 Data 
Utilized Skills: Joins, Common Table Expressions (CTEs), Temporary Tables, Window Functions, Aggregate Functions, View Creation, & Data Type Conversions

SELECT *
FROM PortfolioProjects..CovidDeaths
WHERE continent is not null
ORDER BY 3,4


--Select data we are going to be using
SELECT Location, date, total_cases, new_cases, total_deaths, population
FROM PortfolioProjects..CovidDeaths
WHERE continent is not null
ORDER BY 1,2


--Looking at Total Cases vs. Total Deaths
--Shows likelihood of dying if you contract COVID in your country

SELECT Location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS Death_Percentage
FROM PortfolioProjects..CovidDeaths
--Where location like '%state%'
WHERE continent is not null
ORDER BY 1,2

--This query retrieves data from the CovidDeaths table, including location, date, total cases, total deaths, and the calculated death rate.
--It addresses the issue of operand data type nvarchar being invalid for the divide operator.

--Original query that resulted in an error:
--SELECT Location, date, total_cases, total_deaths, (total_deaths/total_cases)
--FROM PortfolioProjects..CovidDeaths
--WHERE continent is not null
--ORDER BY 1, 2

-- Modified query
SELECT
    Location,
    date,
    total_cases,
    total_deaths,
    -- Calculate death rate only if total_cases and total_deaths are numeric
    CASE
        WHEN ISNUMERIC(total_cases) = 1 AND ISNUMERIC(total_deaths) = 1
            THEN CAST(total_deaths AS FLOAT) / CAST(total_cases AS FLOAT)*100 
        ELSE NULL
    END AS Death_Percentage
FROM PortfolioProjects..CovidDeaths
WHERE location LIKE '%states%'
and continent is not null
ORDER BY 1, 2


--Looking at the Total Cases vs. Population
--Shows what percentage of population contracted COVID
SELECT
    Location,
    date,
	population,
    total_cases,
	(total_cases/population)*100 AS Percent_Population_Infected
FROM PortfolioProjects..CovidDeaths
WHERE location LIKE '%states%'
ORDER BY 1, 2


--Looking at Countries with Highest Infection Rate compared to Population:
SELECT
    Location,
	Population,
    MAX(total_cases) AS Highest_Infection_Rate,
	MAX((total_cases/population))*100 AS Percent_Population_Infected
FROM PortfolioProjects..CovidDeaths
--WHERE location LIKE '%states%'
GROUP BY continent
ORDER BY Percent_Population_Infected desc


--Showing the countries with Highest Death Count per Population
SELECT
    Location, MAX(CAST(total_deaths AS INT)) AS Total_Death_Count
FROM PortfolioProjects..CovidDeaths
--WHERE location LIKE '%states%'
WHERE continent is not null
GROUP BY continent
ORDER BY Total_Death_Count desc

	
--Let's break things down by continent
--Showing continents with the highest death count per population
SELECT
   continent, MAX(CAST(total_deaths AS INT)) AS Total_Death_Count
FROM PortfolioProjects..CovidDeaths
--WHERE location LIKE '%states%'
WHERE continent is not null
GROUP BY continent
ORDER BY Total_Death_Count desc


--Global numbers
SELECT
    SUM(new_cases) AS Total_Cases, SUM(CAST(new_deaths AS INT)) AS Total_Deaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS Death_Percentage
FROM PortfolioProjects..CovidDeaths
--Where location like '%states%'
WHERE continent IS NOT NULL
--GROUP BY date
ORDER BY 1, 2

--Looking at Total Population vs. Vaccinations
--Displays the percentage of the population that has received at least one dose of the COVID-19 vaccine
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(INT, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
FROM PortfolioProjects..CovidDeaths dea
JOIN PortfolioProjects..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2, 3

-- Modified: Adjusted data type to BIGINT to prevent arithmetic overflow error
SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CONVERT(BIGINT, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
--, (Rolling_People_Vaccinated/population) * 100
FROM
    PortfolioProjects..CovidDeaths dea
JOIN
    PortfolioProjects..CovidVaccinations vac
ON
    dea.location = vac.location
    AND dea.date = vac.date
WHERE
    dea.continent IS NOT NULL
ORDER BY
    2, 3


--Using CTE to execute calculations with a PARTITION BY clause in the preceding query
WITH PopvsVac (Continent, Location, Date, Population, New_Vaccinations, Rolling_People_Vaccinated)
AS
(
SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CONVERT(BIGINT, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
--, (Rolling_People_Vaccinated/population) * 100
FROM
    PortfolioProjects..CovidDeaths dea
JOIN
    PortfolioProjects..CovidVaccinations vac
ON
    dea.location = vac.location
    AND dea.date = vac.date
WHERE
    dea.continent IS NOT NULL
--ORDER BY
    2, 3
)
SELECT *, (Rolling_People_Vaccinated/Population) * 100
FROM PopvsVac

-- Modified: Removed ORDER BY clause in CTE
WITH PopvsVac (Continent, Location, Date, Population, New_Vaccinations, Rolling_People_Vaccinated)
AS
(
    SELECT
        dea.continent,
        dea.location,
        dea.date,
        dea.population,
        vac.new_vaccinations,
        SUM(CONVERT(BIGINT, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
    FROM
        PortfolioProjects..CovidDeaths dea
    JOIN
        PortfolioProjects..CovidVaccinations vac
    ON
        dea.location = vac.location
        AND dea.date = vac.date
    WHERE
        dea.continent IS NOT NULL
)
SELECT *, (Rolling_People_Vaccinated/Population) * 100 AS Vaccination_Percentage
FROM PopvsVac
ORDER BY Location, Date; -- Move ORDER BY to the final SELECT statement


--Utilizing a temp table to execute calculations with a `PARTITION BY` clause in the preceding query
DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
    Continent nvarchar(255),
    Location nvarchar(255),
    Date datetime,
    Population numeric,
    NewVaccinations numeric,
    RollingPeopleVaccinated numeric
);

-- Insert data into the table
INSERT INTO #PercentPopulationVaccinated
SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CONVERT(BIGINT, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location) AS RollingPeopleVaccinated
FROM PortfolioProjects..CovidDeaths dea
JOIN PortfolioProjects..CovidVaccinations vac
ON dea.location = vac.location
    AND dea.date = vac.date
--WHERE dea.continent IS NOT NULL
ORDER BY 2, 3;

-- Select data from the table
SELECT *, (RollingPeopleVaccinated / Population) * 100 AS PercentPopulationVaccinated
FROM #PercentPopulationVaccinated


--CREATE VIEW PercentPopulationVaccinated AS
-- Drop the view if it exists
IF OBJECT_ID('PercentPopulationVaccinated', 'V') IS NOT NULL
    DROP VIEW PercentPopulationVaccinated;
GO

-- Creating a view to store data for later visualizations
CREATE VIEW PercentPopulationVaccinated AS 
SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CONVERT(BIGINT, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location) AS RollingPeopleVaccinated
FROM PortfolioProjects..CovidDeaths dea
JOIN PortfolioProjects..CovidVaccinations vac
ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent IS NOT NULL;


SELECT *
FROM PercentPopulationVaccinated














